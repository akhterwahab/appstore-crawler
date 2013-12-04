# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class appItem(Item):
    # define the fields for your item here like:
    id = Field()
    is_del = Field()
    status = Field()
    created_time = Field()
    name = Field()	
    adgroup_id = Field()
    original_id = Field()
    display_type = Field()
    site_name = Field()
    apk_name = Field()
    package_name = Field()
    apk_size = Field()
    app_version = Field()
    app_store_id = Field()
    icon = Field()
    url = Field()
    adPic = Field()
    picSize = Field()
    title = Field()
    adDesc = Field()
    adDescBrief = Field()
    provider = Field()
    screenshots = Field()
    auditStatus = Field()
    refuseReason = Field()
    evaluation = Field()
