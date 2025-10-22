# modules/file_uploader.py
import httpx
import logging

logger = logging.getLogger(__name__)

from typing import Tuple

from typing import Tuple

async def upload_to_file_bed(file_name: str, file_data: str, upload_url: str, api_key: str | None = None) -> Tuple[str | None, str | None]:
    """
    将 base64 编码的文件上传到文件床服务器。

    :param file_name: 原始文件名。
    :param file_data: Base64 data URI (例如, "data:image/png;base64,...").
    :param upload_url: 文件床的 /upload 端点 URL。
    :param api_key: (可选) 用于认证的 API Key。
    :return: 一个元组 (filename, error_message)。成功时 filename 是字符串，error_message 是 None；
             失败时 filename 是 None，error_message 是包含错误信息的字符串。
    """
    payload = {
        "file_name": file_name,
        "file_data": file_data,
        "api_key": api_key
    }
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(upload_url, json=payload)
            
            response.raise_for_status()  # 如果状态码是 4xx 或 5xx，则引发异常
            
            result = response.json()
            if result.get("success") and result.get("filename"):
                logger.info(f"文件 '{file_name}' 成功上传到文件床，文件名为: {result['filename']}")
                return result["filename"], None
            else:
                error_msg = result.get("error", "文件床返回了未知的错误。")
                logger.error(f"上传到文件床失败: {error_msg}")
                return None, error_msg
                
    except httpx.HTTPStatusError as e:
        error_details = f"HTTP 错误: {e.response.status_code} - {e.response.text}"
        logger.error(f"上传到文件床时发生 {error_details}")
        return None, error_details
    except httpx.RequestError as e:
        error_details = f"连接错误: {e}"
        logger.error(f"连接到文件床服务器时出错: {e}")
        return None, error_details
    except Exception as e:
        error_details = f"未知错误: {e}"
        logger.error(f"上传文件时发生未知错误: {e}", exc_info=True)
        return None, error_details
