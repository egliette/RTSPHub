# RTSPHub

A FastAPI-based recording service that connects to RTSP streams, proxies them via MediaMTX, and provides APIs to record and store videos locally or in MinIO.

![RTSP Hub diagram](assets/images/rtsp_hub_diagram.png)

## 1. Overview

RTSPHub is a service designed for camera-related tasks and computer vision applications. It leverages [**MediaMTX**](https://github.com/bluenviron/mediamtx) and provides APIs to retrieve recorded video content based on specific time ranges - a feature not yet available in MediaMTX. This is particularly useful for:

- **Object Detection Tasks**: Extract specific video segments when objects are detected
- **Evidence Collection**: Retrieve video footage for specific time periods
- **Computer Vision Pipelines**: Access historical video data for analysis
- **Surveillance Systems**: Manage multiple camera streams efficiently

## 2. Key Features

1. **Stream Management**: Create proxy streams, monitor health, and manage streams via REST API with automatic recording
2. **Video Retrieval**: Provide API to return requested video based on time range
3. **Storage Options**: Local filesystem or MinIO object storage with presigned URLs
4. **Data Persistence**: Store stream information and task metadata in SQLite

## 3. Installation



## 4. API Endpoints

### Stream Management
- `POST /api/streams` - Create a new proxy stream
- `GET /api/streams` - List all active streams
- `DELETE /api/streams/{stream_id}` - Remove a stream
- `GET /api/streams/{stream_id}/health` - Check stream health

### Video Retrieval
- `POST /api/video-process/tasks` - Create video processing task
- `GET /api/video-process/tasks` - List all video tasks
- `GET /api/video-process/tasks/{task_id}` - Get task status
- `DELETE /api/video-process/tasks/{task_id}` - Delete task

## 5. TODO

- [ ] Update to async implementation
- [ ] Allow recording in the future (end time in the future)
- [ ] Add route to request from + or - from the current time
- [ ] Auto generate documentation
