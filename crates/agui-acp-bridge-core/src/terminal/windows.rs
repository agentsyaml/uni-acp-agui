use std::io;
use std::path::{Path, PathBuf};
use tokio::process::Child;

#[cfg(windows)]
#[derive(Debug)]
pub(super) struct WindowsDirectoryLock {
    handles: Vec<usize>,
}

#[cfg(windows)]
impl WindowsDirectoryLock {
    pub(super) fn open(_root: &Path, canonical: &Path) -> io::Result<Self> {
        let mut ancestors: Vec<PathBuf> = canonical
            .ancestors()
            .filter(|path| !path.as_os_str().is_empty())
            .map(PathBuf::from)
            .collect();
        ancestors.reverse();
        ancestors.dedup();

        let mut lock = Self {
            handles: Vec::with_capacity(ancestors.len()),
        };
        for ancestor in ancestors {
            let handle = open_windows_directory(&ancestor)?;
            let actual = match windows_final_path(handle) {
                Ok(actual) => actual,
                Err(error) => {
                    unsafe {
                        let _ = CloseHandle(handle);
                    }
                    return Err(error);
                }
            };
            if !windows_paths_equal(&actual, &ancestor) {
                unsafe {
                    let _ = CloseHandle(handle);
                }
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "working directory reparse point changed during validation",
                ));
            }
            lock.handles.push(handle as usize);
        }
        Ok(lock)
    }
}

#[cfg(windows)]
impl Drop for WindowsDirectoryLock {
    fn drop(&mut self) {
        for handle in self.handles.drain(..) {
            unsafe {
                let _ = CloseHandle(handle as WindowsHandle);
            }
        }
    }
}

#[cfg(windows)]
fn windows_wide(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;

    if path.as_os_str().encode_wide().any(|unit| unit == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "working directory contains NUL",
        ));
    }
    Ok(path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect())
}

#[cfg(windows)]
fn open_windows_directory(path: &Path) -> io::Result<WindowsHandle> {
    let path = windows_wide(path)?;
    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            FILE_READ_ATTRIBUTES,
            // Do not share delete/rename. Holding every ancestor handle makes
            // the CreateProcess current-directory string stable until spawn.
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        Err(io::Error::last_os_error())
    } else {
        Ok(handle)
    }
}

#[cfg(windows)]
fn windows_final_path(handle: WindowsHandle) -> io::Result<PathBuf> {
    let mut buffer = vec![0_u16; 256];
    loop {
        let length = unsafe {
            GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), buffer.len() as u32, 0)
        };
        if length == 0 {
            return Err(io::Error::last_os_error());
        }
        if length < buffer.len() as u32 {
            return Ok(PathBuf::from(String::from_utf16_lossy(
                &buffer[..length as usize],
            )));
        }
        buffer.resize(length as usize + 1, 0);
    }
}

#[cfg(windows)]
fn windows_paths_equal(left: &Path, right: &Path) -> bool {
    let normalize = |path: &Path| {
        let mut path = path
            .to_string_lossy()
            .replace('/', "\\")
            .to_ascii_lowercase();
        if let Some(unc) = path.strip_prefix(r"\\?\unc\") {
            path = format!(r"\\{unc}");
        } else if let Some(plain) = path.strip_prefix(r"\\?\") {
            path = plain.to_owned();
        }
        path.trim_end_matches('\\').to_owned()
    };
    normalize(left) == normalize(right)
}

#[cfg(windows)]
const FILE_READ_ATTRIBUTES: u32 = 0x80;
#[cfg(windows)]
const FILE_SHARE_READ: u32 = 0x1;
#[cfg(windows)]
const FILE_SHARE_WRITE: u32 = 0x2;
#[cfg(windows)]
const OPEN_EXISTING: u32 = 3;
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
#[cfg(windows)]
const INVALID_HANDLE_VALUE: WindowsHandle = -1_isize as WindowsHandle;
#[cfg(windows)]
pub(super) const CREATE_SUSPENDED: u32 = 0x0000_0004;
#[cfg(windows)]
const THREAD_SUSPEND_RESUME_FAILED: u32 = u32::MAX;
#[cfg(windows)]
const TH32CS_SNAPTHREAD: u32 = 0x0000_0004;
#[cfg(windows)]
const THREAD_SUSPEND_RESUME: u32 = 0x0002;

#[cfg(windows)]
#[repr(C)]
struct ThreadEntry32 {
    size: u32,
    usage: u32,
    thread_id: u32,
    owner_process_id: u32,
    base_priority: i32,
    delta_priority: i32,
    flags: u32,
}

#[cfg(windows)]
unsafe extern "system" {
    fn CreateFileW(
        path: *const u16,
        desired_access: u32,
        share_mode: u32,
        security_attributes: *mut std::ffi::c_void,
        creation_disposition: u32,
        flags_and_attributes: u32,
        template_file: WindowsHandle,
    ) -> WindowsHandle;
    fn GetFinalPathNameByHandleW(
        file: WindowsHandle,
        path: *mut u16,
        path_length: u32,
        flags: u32,
    ) -> u32;
    fn CreateToolhelp32Snapshot(flags: u32, process_id: u32) -> WindowsHandle;
    fn Thread32First(snapshot: WindowsHandle, entry: *mut ThreadEntry32) -> i32;
    fn Thread32Next(snapshot: WindowsHandle, entry: *mut ThreadEntry32) -> i32;
    fn OpenThread(desired_access: u32, inherit_handle: i32, thread_id: u32) -> WindowsHandle;
}

#[cfg(windows)]
#[derive(Debug)]
pub(super) struct WindowsJob {
    handle: usize,
}

#[cfg(windows)]
impl WindowsJob {
    pub(super) fn supported() -> bool {
        let handle = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if handle.is_null() {
            return false;
        }
        let supported = Self::configure(handle).is_ok();
        unsafe {
            let _ = CloseHandle(handle);
        }
        supported
    }

    fn configure(handle: WindowsHandle) -> io::Result<()> {
        let mut limits = JobObjectBasicLimitInformation {
            per_process_user_time_limit: 0,
            per_job_user_time_limit: 0,
            limit_flags: JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            minimum_working_set_size: 0,
            maximum_working_set_size: 0,
            active_process_limit: 0,
            affinity: 0,
            priority_class: 0,
            scheduling_class: 0,
        };
        if unsafe {
            SetInformationJobObject(
                handle,
                JOB_OBJECT_BASIC_LIMIT_INFORMATION_CLASS,
                (&mut limits as *mut JobObjectBasicLimitInformation).cast(),
                std::mem::size_of::<JobObjectBasicLimitInformation>() as u32,
            )
        } == 0
        {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    pub(super) fn for_child(child: &Child) -> std::io::Result<Self> {
        let handle = unsafe { CreateJobObjectW(std::ptr::null_mut(), std::ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        let job = Self {
            handle: handle as usize,
        };
        Self::configure(job.raw_handle())?;
        let process = child
            .raw_handle()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "child exited"))?;
        if unsafe { AssignProcessToJobObject(job.raw_handle(), process) } == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(job)
    }

    fn raw_handle(&self) -> WindowsHandle {
        self.handle as WindowsHandle
    }

    pub(super) fn terminate(&self) -> std::io::Result<()> {
        if unsafe { TerminateJobObject(self.raw_handle(), 1) } != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    pub(super) fn resume(&self, child: &Child) -> std::io::Result<()> {
        let pid = child
            .id()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "child exited"))?;
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }

        let mut entry = ThreadEntry32 {
            size: std::mem::size_of::<ThreadEntry32>() as u32,
            usage: 0,
            thread_id: 0,
            owner_process_id: 0,
            base_priority: 0,
            delta_priority: 0,
            flags: 0,
        };
        let mut result = Err(io::Error::new(
            io::ErrorKind::NotFound,
            "suspended child thread not found",
        ));
        let mut found = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while found {
            if entry.owner_process_id == pid {
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.thread_id) };
                if !thread.is_null() {
                    let resumed = unsafe { ResumeThread(thread) };
                    unsafe {
                        let _ = CloseHandle(thread);
                    }
                    if resumed != THREAD_SUSPEND_RESUME_FAILED {
                        result = Ok(());
                        break;
                    }
                    result = Err(io::Error::last_os_error());
                }
            }
            found = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        unsafe {
            let _ = CloseHandle(snapshot);
        }
        result
    }
}

#[cfg(windows)]
impl Drop for WindowsJob {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.raw_handle());
        }
    }
}

#[cfg(windows)]
type WindowsHandle = *mut std::ffi::c_void;

#[cfg(windows)]
const JOB_OBJECT_BASIC_LIMIT_INFORMATION_CLASS: u32 = 2;

#[cfg(windows)]
const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x2000;

#[cfg(windows)]
#[repr(C)]
struct JobObjectBasicLimitInformation {
    per_process_user_time_limit: i64,
    per_job_user_time_limit: i64,
    limit_flags: u32,
    minimum_working_set_size: usize,
    maximum_working_set_size: usize,
    active_process_limit: u32,
    affinity: usize,
    priority_class: u32,
    scheduling_class: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateJobObjectW(job_attributes: *mut std::ffi::c_void, name: *const u16) -> WindowsHandle;
    fn SetInformationJobObject(
        job: WindowsHandle,
        information_class: u32,
        information: *mut std::ffi::c_void,
        information_length: u32,
    ) -> i32;
    fn AssignProcessToJobObject(job: WindowsHandle, process: WindowsHandle) -> i32;
    fn TerminateJobObject(job: WindowsHandle, exit_code: u32) -> i32;
    fn ResumeThread(thread: WindowsHandle) -> u32;
    fn CloseHandle(handle: WindowsHandle) -> i32;
}
