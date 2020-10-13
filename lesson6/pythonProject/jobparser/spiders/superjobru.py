import scrapy
from scrapy.http import HtmlResponse
from jobparser.items import JobparserItem


class SuperjobruSpider(scrapy.Spider):
    name = 'superjobru'
    allowed_domains = ['superjob.ru']
    start_urls = ['https://www.superjob.ru/vacancy/search/?keywords=python&geo%5Bt%5D%5B0%5D=4']

    def parse(self, response: HtmlResponse):
        #vacancies = response.css("div.3mfro.PlM3e._2JVkc._3LJqf::attr(href)").extract()
        vacancies = response.xpath("//div[@class='_3mfro PlM3e _2JVkc _3LJqf']//a/@href").extract()

        for vacancy in vacancies:
            yield response.follow(vacancy, callback=self.vacancy_parse)

        next_page = response.css("a.icMQ_._1_Cht._3ze9n.f-test-button-dalshe.f-test-link-Dalshe::attr(href)").extract_first()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def vacancy_parse(self, response: HtmlResponse):
        name = response.xpath("//h1/text()").extract_first()
       # salary = response.xpath("//span[@class=‘_3mfro _2Wp8I PlM3e _2JVkc’]/text()").extract()
        salary = response.css("span._3mfro._2Wp8I.PlM3e._2JVkc::text").extract()

        link = response.url

        yield JobparserItem(name=name, salary=salary, link=link)
        print()

