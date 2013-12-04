# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class Creative(Item):
    # define the fields for your item here like:
    id = Field()
    name = Field()	
    display_type = Field()

    #app information
    app_version = Field()
    app_name = Field()
    package_name = Field()
    package_size = Field()
    app_store_id = Field()

    #artist
    provider = Field()

    #pic, icon is small, ad_pic is big
    icon = Field()
    ad_pic = Field()
    screenshots = Field()

    #landing page
    url = Field()

    #app introduction
    title = Field()
    ad_desc = Field()
    ad_desc_brief = Field()

    

   
