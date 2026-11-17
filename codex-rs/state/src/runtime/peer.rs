use super::StateRuntime;
use crate::sqlite::SqlitePoolWriteExt;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use sqlx::Row;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PeerRoute {
    pub product: String,
    #[serde(rename = "sessionID")]
    pub session_id: String,
    pub data_context: String,
    pub locator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerReply {
    pub sequence: i64,
    #[serde(rename = "messageID")]
    pub message_id: String,
    pub message: String,
    pub r#final: bool,
    pub delivery: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerTask {
    pub receipt: String,
    pub source: PeerRoute,
    pub target: PeerRoute,
    #[serde(rename = "messageID")]
    pub message_id: String,
    pub message: String,
    pub admission: String,
    pub state: String,
    pub replies: Vec<PeerReply>,
}

impl StateRuntime {
    pub async fn peer_context(&self) -> Result<String> {
        Ok(
            sqlx::query_scalar("SELECT identity FROM peer_context WHERE id = 1")
                .fetch_one(self.pool.as_ref())
                .await?,
        )
    }

    pub async fn peer_prepare(&self, task: &PeerTask) -> Result<bool> {
        let inserted = sqlx::query("INSERT INTO peer_tasks (receipt, payload_json) VALUES (?, ?) ON CONFLICT(receipt) DO NOTHING")
            .bind(&task.receipt)
            .bind(serde_json::to_string(task)?)
            .execute(self.pool.as_ref())
            .await?;
        Ok(inserted.rows_affected() == 1)
    }

    pub async fn peer_task(&self, receipt: &str) -> Result<PeerTask> {
        let payload: Option<String> =
            sqlx::query_scalar("SELECT payload_json FROM peer_tasks WHERE receipt = ?")
                .bind(receipt)
                .fetch_optional(self.pool.as_ref())
                .await?;
        let Some(payload) = payload else {
            bail!("receipt_not_found: Receipt not found in this data context");
        };
        Ok(serde_json::from_str(&payload)?)
    }

    pub async fn peer_admission(&self, receipt: &str, admission: &str) -> Result<()> {
        self.peer_update(receipt, |task| {
            task.admission = admission.to_string();
            Ok(())
        })
        .await?;
        Ok(())
    }

    pub async fn peer_append(
        &self,
        receipt: &str,
        message: &str,
        final_reply: bool,
    ) -> Result<(PeerTask, PeerReply)> {
        let task = self
            .peer_update(receipt, |task| {
                if task.admission != "accepted" {
                    bail!("admission_failed: Task admission has not been confirmed");
                }
                if task.replies.iter().any(|reply| {
                    reply.delivery == "pending"
                        && reply.message == message
                        && reply.r#final == final_reply
                }) {
                    return Ok(());
                }
                if task.state == "closed" {
                    bail!("task_closed: The final reply has already been saved");
                }
                task.replies.push(PeerReply {
                    sequence: i64::try_from(task.replies.len())? + 1,
                    message_id: format!("msg_{}", uuid::Uuid::now_v7()),
                    message: message.to_string(),
                    r#final: final_reply,
                    delivery: "pending".to_string(),
                });
                if final_reply {
                    task.state = "closed".to_string();
                }
                Ok(())
            })
            .await?;
        let reply = task
            .replies
            .iter()
            .find(|reply| {
                reply.delivery == "pending"
                    && reply.message == message
                    && reply.r#final == final_reply
            })
            .ok_or_else(|| anyhow::anyhow!("persistence_failed: Reply was not saved"))?
            .clone();
        Ok((task, reply))
    }

    pub async fn peer_delivered(&self, receipt: &str, sequence: i64) -> Result<()> {
        self.peer_update(receipt, |task| {
            if let Some(reply) = task
                .replies
                .iter_mut()
                .find(|reply| reply.sequence == sequence)
            {
                reply.delivery = "queued".to_string();
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn peer_update(
        &self,
        receipt: &str,
        change: impl FnOnce(&mut PeerTask) -> Result<()>,
    ) -> Result<PeerTask> {
        let mut tx = self.pool.begin_immediate().await?;
        let row = sqlx::query("SELECT payload_json FROM peer_tasks WHERE receipt = ?")
            .bind(receipt)
            .fetch_optional(tx.as_mut())
            .await?;
        let Some(row) = row else {
            bail!("receipt_not_found: Receipt not found in this data context");
        };
        let mut task: PeerTask = serde_json::from_str(row.get("payload_json"))?;
        change(&mut task)?;
        sqlx::query("UPDATE peer_tasks SET payload_json = ? WHERE receipt = ?")
            .bind(serde_json::to_string(&task)?)
            .bind(receipt)
            .execute(tx.as_mut())
            .await?;
        tx.commit().await?;
        Ok(task)
    }
}
