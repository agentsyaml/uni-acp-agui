use super::*;
use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, ImageContent, Plan, PlanEntry, PlanEntryPriority, PlanEntryStatus,
    TextContent, ToolCall, ToolCallId, ToolCallUpdate, ToolCallUpdateFields,
};

fn chunk(text: &str) -> ContentChunk {
    ContentChunk::new(ContentBlock::Text(TextContent::new(text)))
}

fn chunk_with_id(text: &str, message_id: &str) -> ContentChunk {
    chunk(text).message_id(message_id)
}

mod bounds;
mod metadata;
mod plans;
mod text;
mod tools;
