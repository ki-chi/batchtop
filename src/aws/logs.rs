use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_cloudwatchlogs::operation::get_log_events::GetLogEventsOutput;

use super::{LogsApi, RealAws};
use crate::domain::{ApiError, LogDirection, LogEvent, LogLocation, LogPage};

#[async_trait]
impl LogsApi for RealAws {
    async fn get_log_events(
        &self,
        location: &LogLocation,
        direction: LogDirection,
        next_token: Option<String>,
    ) -> Result<LogPage, ApiError> {
        let _permit = self.request_permit().await;
        let config = aws_sdk_cloudwatchlogs::config::Builder::from(&self.shared_config)
            .region(Region::new(location.region.clone()))
            .build();
        let client = aws_sdk_cloudwatchlogs::Client::from_conf(config);
        let start_from_head = matches!(direction, LogDirection::Forward);
        let output = client
            .get_log_events()
            .log_group_name(&location.group)
            .log_stream_name(&location.stream)
            .limit(1_000)
            .start_from_head(start_from_head)
            .set_next_token(next_token)
            .send()
            .await
            .map_err(|error| ApiError::new("CloudWatch Logs GetLogEvents", error.to_string()))?;

        Ok(map_output(&output))
    }
}

fn map_output(output: &GetLogEventsOutput) -> LogPage {
    LogPage {
        events: output
            .events()
            .iter()
            .map(|event| LogEvent {
                timestamp: event
                    .timestamp()
                    .and_then(chrono::DateTime::from_timestamp_millis),
                ingestion_time: event
                    .ingestion_time()
                    .and_then(chrono::DateTime::from_timestamp_millis),
                message: event.message().unwrap_or_default().to_owned(),
            })
            .collect(),
        next_backward_token: output.next_backward_token().map(ToOwned::to_owned),
        next_forward_token: output.next_forward_token().map(ToOwned::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_cloudwatchlogs::types::OutputLogEvent;

    use super::*;

    #[test]
    fn maps_events_and_both_pagination_tokens() {
        let output = GetLogEventsOutput::builder()
            .events(
                OutputLogEvent::builder()
                    .timestamp(1_000)
                    .ingestion_time(2_000)
                    .message("hello")
                    .build(),
            )
            .next_backward_token("back")
            .next_forward_token("forward")
            .build();
        let page = map_output(&output);
        assert_eq!(page.events[0].message, "hello");
        assert_eq!(
            page.events[0].timestamp,
            chrono::DateTime::from_timestamp_millis(1_000)
        );
        assert_eq!(page.next_backward_token.as_deref(), Some("back"));
        assert_eq!(page.next_forward_token.as_deref(), Some("forward"));
    }
}
