import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from interface_grafica.services.update_service import UpdateWorker, UpdateService, ReleaseInfo
from PySide6.QtNetwork import QNetworkReply

def test_version_comparison():
    worker = UpdateWorker()
    assert worker.is_newer("v0.2.0", "0.1.0") is True
    assert worker.is_newer("0.1.1", "0.1.0") is True
    assert worker.is_newer("0.1.0", "0.1.0") is False
    assert worker.is_newer("0.0.9", "0.1.0") is False

@patch("interface_grafica.services.update_service.QNetworkAccessManager")
@patch("interface_grafica.services.update_service.QEventLoop")
def test_update_worker_no_update(mock_loop, mock_manager_class):
    mock_manager = mock_manager_class.return_value
    mock_reply = MagicMock(spec=QNetworkReply)
    mock_reply.error.return_value = QNetworkReply.NoError
    mock_reply.readAll().data().decode.return_value = '{"tag_name": "0.1.0"}'
    mock_manager.get.return_value = mock_reply

    worker = UpdateWorker()

    with patch.object(worker, "finished") as mock_finished:
        worker.run()
        mock_finished.emit.assert_called_once_with(None)

@patch("interface_grafica.services.update_service.QNetworkAccessManager")
@patch("interface_grafica.services.update_service.QEventLoop")
def test_update_worker_with_update(mock_loop, mock_manager_class):
    mock_manager = mock_manager_class.return_value
    mock_reply = MagicMock(spec=QNetworkReply)
    mock_reply.error.return_value = QNetworkReply.NoError
    mock_reply.readAll().data().decode.return_value = '''
    {
        "tag_name": "0.2.0",
        "body": "New features",
        "assets": [
            {"name": "app.zip", "browser_download_url": "http://example.com/app.zip"},
            {"name": "checksums.txt", "browser_download_url": "http://example.com/checksums.txt"}
        ]
    }
    '''
    mock_manager.get.return_value = mock_reply

    worker = UpdateWorker()

    with patch.object(worker, "finished") as mock_finished:
        worker.run()
        args, _ = mock_finished.emit.call_args
        release_info = args[0]
        assert isinstance(release_info, ReleaseInfo)
        assert release_info.tag_name == "0.2.0"
        assert release_info.zip_url == "http://example.com/app.zip"
        assert release_info.checksums_url == "http://example.com/checksums.txt"

def test_hash_verification(tmp_path):
    zip_file = tmp_path / "test.zip"
    zip_file.write_bytes(b"test content")

    import hashlib
    expected_hash = hashlib.sha256(b"test content").hexdigest()

    service = UpdateService()
    assert service.verify_hash(str(zip_file), expected_hash) is True
    assert service.verify_hash(str(zip_file), "wrong hash") is False

@patch("interface_grafica.services.update_service.QNetworkAccessManager")
def test_full_download_flow_with_hash(mock_manager_class, tmp_path):
    mock_manager = mock_manager_class.return_value

    # Setup mocks for checksums and zip download
    mock_reply_checksums = MagicMock(spec=QNetworkReply)
    mock_reply_checksums.error.return_value = QNetworkReply.NoError

    import hashlib
    zip_content = b"fake zip content"
    expected_hash = hashlib.sha256(zip_content).hexdigest()
    mock_reply_checksums.readAll().data().decode.return_value = f"{expected_hash}  app.zip"

    mock_reply_zip = MagicMock(spec=QNetworkReply)
    mock_reply_zip.error.return_value = QNetworkReply.NoError
    mock_reply_zip.readAll().data.return_value = zip_content

    # First call is for checksums, second for zip
    mock_manager.get.side_effect = [mock_reply_checksums, mock_reply_zip]

    service = UpdateService()
    release_info = ReleaseInfo(
        tag_name="0.2.0",
        body="Update",
        zip_url="http://example.com/app.zip",
        checksums_url="http://example.com/checksums.txt"
    )

    with patch.object(service, "download_finished") as mock_finished:
        # Mock the finished.connect calls to capture callbacks
        checksums_callback = None
        def mock_connect_checksums(cb):
            nonlocal checksums_callback
            checksums_callback = cb
        mock_reply_checksums.finished.connect.side_effect = mock_connect_checksums

        zip_callback = None
        def mock_connect_zip(cb):
            nonlocal zip_callback
            zip_callback = cb
        mock_reply_zip.finished.connect.side_effect = mock_connect_zip

        service.start_update_download(release_info)

        # Trigger checksums finished manually via captured callback
        checksums_callback()

        # Trigger zip finished manually via captured callback
        zip_callback()

        mock_finished.emit.assert_called_once()
        zip_path = mock_finished.emit.call_args[0][0]
        assert Path(zip_path).read_bytes() == zip_content

@patch("shutil.unpack_archive")
def test_prepare_swap_script(mock_unpack, tmp_path):
    zip_file = tmp_path / "update.zip"
    zip_file.write_bytes(b"dummy zip")

    service = UpdateService()
    bat_path = service.prepare_swap_script(str(zip_file))

    assert Path(bat_path).exists()
    assert bat_path.endswith(".bat")

    content = Path(bat_path).read_text(encoding="latin-1")
    assert "BACKUP_DIR=%APP_DIR%.bak" in content
    assert 'if /i "%%f"=="dados" set skip=1' in content
    assert "tasklist /fi" in content
