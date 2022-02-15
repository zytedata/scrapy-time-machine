import scrapy


class RandomSpider(scrapy.Spider):
    name = 'random'
    allowed_domains = ['random.org']
    start_urls = [
        "https://www.random.org/integers/?num=1&min=1&max=100&col=1&base=10&format=html&rnd=new&cl=w"
    ]

    def parse(self, response):
        return {
            "number": response.css(".data::text").get("").strip(),
            "timestamp": response.css("p::text").re_first("Timestamp: .*"),
        }
