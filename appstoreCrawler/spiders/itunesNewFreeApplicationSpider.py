from scrapy.spider import BaseSpider
from scrapy.selector import XmlXPathSelector
from scrapy.http import Request, Response
from scrapy import log
from appstoreCrawler.items import itunesItem
from appstoreCrawler.items import creative, extension
from scrapy.contrib.loader.processor import TakeFirst, MapCompose, Join
import json

class itunesNewFreeApplicationSpider(BaseSpider):

    extension_schema = { "terms" : "genres", \
			 "features" : "features", \
			 "supportedDevices" : "supportedDevices", \
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
	    lookup_result = lookup_results["results"][0]
	    iItem["creative"] = creative.Creative()
	    iItem["creative"]["name"] = lookup_result["trackName"].encode("utf8")
	    iItem["creative"]["app_version"] = lookup_result["version"].encode("utf8")
	    iItem["creative"]["app_name"] = lookup_result["trackName"].encode("utf8")
	    iItem["creative"]["package_name"] = lookup_result["bundleId"].encode("utf8")
	    iItem["creative"]["package_size"] = str(lookup_result["fileSizeBytes"])
	    iItem["creative"]["app_store_id"] = str(lookup_result["trackId"])
	    iItem["creative"]["provider"] = lookup_result["artistName"].encode("utf8")
	    iItem["creative"]["icon"] = lookup_result["artworkUrl60"].encode("utf8")
	    iItem["creative"]["ad_pic"] = lookup_result["artworkUrl512"].encode("utf8")
	    iItem["creative"]["screenshots"] = str(",".join(lookup_result["screenshotUrls"]))
	    iItem["creative"]["url"] = lookup_result["trackViewUrl"].encode("utf8")
	    iItem["creative"]["title"] = lookup_result["trackName"].encode("utf8")
	    iItem["creative"]["ad_desc"] = lookup_result["description"].encode("utf8")
	    iItem["creative"]["ad_desc_brief"] = lookup_result["description"].partition('\n')[0].encode("utf8")

	    iItem["extension"] = extension.Extension()
	    for key in itunesNewFreeApplicationSpider.extension_schema.keys():
	        iItem["extension"][key] = lookup_result[itunesNewFreeApplicationSpider.extension_schema[key]]

	else:
	    log.msg("Fetch url: %s with no result" % (response.meta["url"]), level=log.WARNING)
        return iItem
