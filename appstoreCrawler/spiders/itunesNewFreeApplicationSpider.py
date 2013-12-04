from scrapy.spider import BaseSpider
from scrapy.selector import XmlXPathSelector
from scrapy.http import Request, Response
from scrapy import log
from appstoreCrawler.items import itunesItem
from appstoreCrawler.items import creative, extension
from scrapy.contrib.loader.processor import TakeFirst, MapCompose, Join
import json

class itunesNewFreeApplicationSpider(BaseSpider):
    creative_schema = { "name" : "trackName", \
	      	        "app_version" : "version", \
		        "app_name" : "trackName", \
		        "package_name" : "bundleId", \
			"package_size" : "fileSizeBytes", \
			"app_store_id" : "trackId", \
			"provider" : "artistName", \
			"icon" : "artworkUrl60", \
			"ad_pic" : "artworkUrl512", \
			"screenshots" : "screenshotUrls", \
			"url" : "trackViewUrl", \
			"title" : "trackName", \
			"ad_desc" : "description", \
			"ad_desc_brief" : "description" \
			}

    extension_schema = { "terms" : "genres", \
			 "features" : "features" \
		       }
 
    url_pattern = "http://itunes.apple.com/lookup?id=%s&country=CN"
    name = "itunesNewFreeApplication"
    allowed_domains = ["apple.com"]
    start_urls = [
	"https://itunes.apple.com/cn/rss/newfreeapplications/limit=300/json"
    ]

    def parse(self, response):
	try:
	    response_content = json.loads(response.body)
	    if response_content.has_key("feed") \
	        and response_content["feed"].has_key("entry"):
	        entries = response_content["feed"]["entry"]
	        
	        for entry in entries:
	    	    if entry.has_key("id") \
	    	        and entry["id"].has_key("attributes") \
	    	        and entry["id"]["attributes"].has_key("im:id"):
	                    id = entry["id"]["attributes"]["im:id"]
	        	    url = itunesNewFreeApplicationSpider.url_pattern % (id)
	        	    yield Request(url, meta = {}, callback = self.parse_itunes_item)
	    else:
	        log.msg("Fetch url: %s encount without entry" % url , level=log.WARNING)
	except Exception, e:
	    log.msg("Fetch url: %s encount a exception: %s" % (response.url, e), level=log.WARNING)


    def parse_itunes_item(self, response):
        lookup_results = json.loads(response.body)
	iItem = itunesItem.itunesItem()
        if lookup_results.has_key("results")  \
	    and lookup_results.has_key("resultCount") \
	    and lookup_results["resultCount"] >= 1:
	    #try:
	        lookup_result = lookup_results["results"][0]
		iItem["creative"] = creative.Creative()
	        for key in itunesNewFreeApplicationSpider.creative_schema.keys():
	            iItem["creative"][key] = lookup_result[itunesNewFreeApplicationSpider.creative_schema[key]]
		iItem["extension"] = extension.Extension()
		for key in itunesNewFreeApplicationSpider.extension_schema.keys():
	            iItem["extension"][key] = lookup_result[itunesNewFreeApplicationSpider.extension_schema[key]]
		
	    #except Exception, e:
	    #    log.msg("Create itunesItem exception: %s" % (e), level=log.WARNING)
	else:
	    log.msg("Fetch url: %s with no result" % (response.meta["url"]), level=log.WARNING)
        return iItem
