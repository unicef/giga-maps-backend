import logging
import requests
from django.conf import settings

logger = logging.getLogger('gigamaps.' + __name__)


class SlackNotificationService:
    """Slack Notification Service"""
    def __init__(self):
        self.school_master_data_changes_webhook_url = settings.SCHOOL_MASTER_DATA_CHANGES_SLACK_WEBHOOK_URL

    def send_school_master_update_notification(self, change_summary, is_published=False):
        """send notifications"""
        if not self.school_master_data_changes_webhook_url:
            logger.info("Slack notifications webhook url not provided.")
            return
        message = None
        try:
            message = self._format_master_release_message(change_summary, is_published)
            self._send_slack_message(self.school_master_data_changes_webhook_url, message)
            logger.info(f"Slack notification sent for {change_summary['country']} master data update")
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}. Nofitication: {message}")
            raise e

    def _format_master_release_message(self, change_summary, is_published):
        """Format message with multiple blocks if needed based on block size."""
        country_name = change_summary['country'].upper()
        header_text = f"*{country_name} MASTER DATA UPDATE*"
        if is_published:
            header_text = f"*{country_name} MASTER DATA UPDATES PUBLISHED*"
        metadata_text = (
            f"*Environment:* {settings.APP_ENVIRONMENT}\n"
            f"*Country:* {change_summary['country']}\n"
            f"*New Rows:* {change_summary['new_rows_count']}\n"
            f"*Updated Rows:* {change_summary['updated_rows_count']}\n"
            f"*Deleted Rows:* {change_summary['deleted_rows_count']}\n"
            f"*Updated:* {change_summary['updated']}\n"
            f"*Pulled At:* {change_summary['pulled_at']}\n"
        )

        blocks = []
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": metadata_text}})

        if change_summary['column_changes']:
            table_header = "*Column changes*\n```"
            table_rows = ["Column                              Count"]
            table_rows.append("-" * 44)

            # Create all rows first
            for change in change_summary['column_changes']:
                column = change['column']
                count = change['count']
                row = f"{column:<36} {count}"
                table_rows.append(row)

            # Split the rows into chunks that fit within Slack's limits
            MAX_BLOCK_SIZE = 3000
            current_block_rows = []
            current_size = len(table_header) + len("```\n")

            for row in table_rows:
                row_size = len(row) + 1
                if current_size + row_size > MAX_BLOCK_SIZE and current_block_rows:
                    # Add current block and reset
                    blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": table_header + "\n".join(current_block_rows) + "```"
                        }
                    })
                    current_block_rows = []
                    current_size = len(table_header) + len("```\n")

                current_block_rows.append(row)
                current_size += row_size
            if current_block_rows:
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": table_header + "\n".join(current_block_rows) + "```"
                    }
                })
        else:
            # If no column changes, add a single block
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*No column changes detected*"
                }
            })

        return {
            "text": "",
            "blocks": blocks
        }

    def _send_slack_message(self, url, message):
        """Send message to Slack webhook."""
        response = requests.post(
            url,
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        response.raise_for_status()
