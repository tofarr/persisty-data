from servey.action.action import action

from persisty_data.img.img_resize_action import resized_img
from servey.servey_web_page.web_page_trigger import WebPageTrigger

resized_img_action = action(resized_img, triggers=WebPageTrigger(path='/resized-img'))
