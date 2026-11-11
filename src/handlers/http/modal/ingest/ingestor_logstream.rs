/*
 * Parseable Server (C) 2022 - 2025 Parseable, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

use actix_web::http::StatusCode;
use actix_web::{
    HttpRequest, HttpResponse, Responder,
    web::{Json, Path},
};
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::storage::object_storage::{
    STREAM_DELETION_ID_HEADER, StreamDeletionTombstone, prepare_stream_deletion,
    read_stream_deletion,
};
use crate::{
    catalog::remove_manifest_from_snapshot,
    handlers::http::logstream::error::StreamError,
    parseable::{PARSEABLE, StreamNotFound},
    utils::get_tenant_id_from_request,
};

// Serializes creation/update and local deletion cleanup on this ingestor.
// Reconciliation uses the same lock so old cleanup cannot remove files from
// a dataset recreated under the same name.
pub(crate) static CREATE_STREAM_LOCK: Mutex<()> = Mutex::const_new(());

pub async fn retention_cleanup(
    req: HttpRequest,
    stream_name: Path<String>,
    Json(date_list): Json<Vec<String>>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let storage = PARSEABLE.storage().get_object_store();
    let tenant_id = get_tenant_id_from_request(&req);
    // if the stream not found in memory map,
    //check if it exists in the storage
    //create stream and schema from storage
    if !PARSEABLE.streams.contains(&stream_name, &tenant_id)
        && !PARSEABLE
            .create_stream_and_schema_from_storage(&stream_name, &tenant_id)
            .await
            .unwrap_or(false)
    {
        return Err(StreamNotFound(stream_name.clone()).into());
    }

    if let Err(err) =
        remove_manifest_from_snapshot(&storage, &stream_name, date_list, &tenant_id).await
    {
        return Err(StreamError::Custom {
            msg: format!(
                "failed to update snapshot during retention cleanup for stream {}: {}",
                stream_name, err
            ),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        });
    }

    Ok(actix_web::HttpResponse::NoContent().finish())
}

pub async fn delete(
    req: HttpRequest,
    stream_name: Path<String>,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let _creation_guard = CREATE_STREAM_LOCK.lock().await;
    let deletion_id = req
        .headers()
        .get(STREAM_DELETION_ID_HEADER)
        .and_then(|id| id.to_str().ok())
        .ok_or_else(|| StreamError::Custom {
            msg: "Missing deletion acknowledgement ID".to_owned(),
            status: StatusCode::BAD_REQUEST,
        })?;
    let storage = PARSEABLE.storage.get_object_store();
    let marker = read_stream_deletion(storage.as_ref(), &stream_name, &tenant_id)
        .await?
        .ok_or_else(|| StreamError::Custom {
            msg: "Deletion is no longer active".to_owned(),
            status: StatusCode::CONFLICT,
        })?;
    let deletion: StreamDeletionTombstone = serde_json::from_slice(&marker)?;
    if deletion.id != deletion_id || !deletion.participants_sealed {
        return Err(StreamError::Custom {
            msg: "Stale or unsealed deletion acknowledgement request".to_owned(),
            status: StatusCode::CONFLICT,
        });
    }

    prepare_stream_deletion(&stream_name, &tenant_id)
        .await
        .map_err(|err| StreamError::Custom {
            msg: format!("Failed to prepare deletion of {stream_name}: {err}"),
            status: StatusCode::INTERNAL_SERVER_ERROR,
        })?;
    if read_stream_deletion(storage.as_ref(), &stream_name, &tenant_id)
        .await?
        .as_ref()
        != Some(&marker)
    {
        return Err(StreamError::Custom {
            msg: "Deletion changed during acknowledgement".to_owned(),
            status: StatusCode::CONFLICT,
        });
    }
    // Keep resident handles marked deleting. Startup also checks tombstones
    // before loading streams, so a restart cannot reactivate this deletion.
    Ok(HttpResponse::Accepted()
        .insert_header((STREAM_DELETION_ID_HEADER, deletion_id))
        .body(format!("log stream {stream_name} deletion prepared")))
}

pub async fn put_stream(
    req: HttpRequest,
    stream_name: Path<String>,
    body: Bytes,
) -> Result<impl Responder, StreamError> {
    let stream_name = stream_name.into_inner();
    let tenant_id = get_tenant_id_from_request(&req);
    let _guard = CREATE_STREAM_LOCK.lock().await;
    PARSEABLE
        .create_update_stream(req.headers(), &body, &stream_name, &tenant_id)
        .await?;

    Ok(("Log stream created", StatusCode::OK))
}
