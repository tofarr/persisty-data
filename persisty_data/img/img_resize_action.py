from typing import Optional, Tuple, Callable

from persisty.errors import PersistyError
from servey.action.action import action
from servey.security.authorization import Authorization
from servey.servey_web_page.redirect import Redirect
from servey.servey_web_page.web_page_trigger import WebPageTrigger
from servey.trigger.web_trigger import WEB_GET

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
    file_name: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    content_type: Optional[str] = None,
    authorization: Optional[Authorization] = None,
) -> Redirect:
    download_url = get_img_resizer().get_resized_image_download_url(
        store_name, file_name, width, height, content_type, authorization
    )
    if download_url is None:
        raise PersistyError("not_found")
    return Redirect(download_url, 301)


def resized_img_url(
    store_name: str,
    file_name: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    content_type: Optional[str] = None,
    authorization: Optional[Authorization] = None,
) -> Optional[str]:
    download_url = get_img_resizer().get_resized_image_download_url(
        store_name, file_name, width, height, content_type, authorization
    )
    return download_url


def create_actions() -> Tuple[Callable, Callable]:
    return (
        action(resized_img, triggers=WebPageTrigger(path="/resized-img")),
        action(resized_img_url, triggers=WEB_GET),
    )
