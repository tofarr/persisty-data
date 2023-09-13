from typing import Optional

from persisty.errors import PersistyError
from servey.security.authorization import Authorization
from servey.servey_web_page.redirect import Redirect

from persisty_data.img.img_resizer import ImgResizer

img_resizer = None


def get_img_resizer():
    global img_resizer
    if img_resizer is None:
        img_resizer = ImgResizer()
    return img_resizer


def set_img_resizer(img_resizer_: ImgResizer):
    global img_resizer
    img_resizer = img_resizer_


def resized_img(
    store_name: str,
    key: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    content_type: Optional[str] = None,
    authorization: Optional[Authorization] = None
) -> Redirect:
    download_url = get_img_resizer().get_resized_image_download_url(store_name, key, width, height, content_type, authorization)
    if download_url is None:
        raise PersistyError('not_found')
    return Redirect(download_url, 301)


def resized_img_url(
    store_name: str,
    key: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    content_type: Optional[str] = None,
    authorization: Optional[Authorization] = None
) -> Optional[str]:
    download_url = get_img_resizer().get_resized_image_download_url(
        store_name, key, width, height, content_type, authorization
    )
    return download_url
