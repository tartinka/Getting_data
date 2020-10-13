# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

from pymongo import MongoClient

class JobparserPipeline:

    def __init__(self):
        client = MongoClient('localhost',27017)
        self.mongo_base = client.vacancies_python

    def process_item(self, item, spider):


        salary = item['salary']
        if spider.name == "superjobru":
            if salary[0] == 'По договорённости':
                item['min_salary'] = 'none'
                item['max_salary'] = 'none'
                item['currency'] = 'none'
            elif salary[0] == 'от':
                item['min_salary'] = int(salary[2].split('\xa0')[0] + salary[2].split('\xa0')[1])
                item['max_salary'] = 'none'
                item['currency'] = salary[2].split('\xa0')[2]
            elif salary[0] == 'до':
                item['min_salary'] = 'none'
                item['max_salary'] = int(salary[2].split('\xa0')[0] + salary[2].split('\xa0')[1])
                item['currency'] = salary[2].split('\xa0')[2]
            else:
                item['min_salary'] = int(salary[0].replace("\xa0",""))
                item['max_salary'] = int(salary[1].replace("\xa0",""))
                item['currency'] = salary[3]

        if spider.name == "hhru":
            if salary[0] == 'от ' and salary[2] == ' до ':
                item['min_salary'] = int(salary[1].replace("\xa0",""))
                item['max_salary'] = int(salary[3].replace("\xa0",""))
                item['currency'] = salary[5]
            elif salary[0] == 'от ' and salary[2] == ' ':
                item['min_salary'] = int(salary[2].split('\xa0')[0] + salary[2].split('\xa0')[1])
                item['max_salary'] = 'none'
                item['currency'] = salary[3]
            elif salary[0] == 'от ' and salary[2] == ' ':
                item['min_salary'] = 'none'
                item['max_salary'] = int(salary[2].split('\xa0')[0] + salary[2].split('\xa0')[1])
                item['currency'] = salary[3]
            else:
                item['min_salary'] = 'none'
                item['max_salary'] = 'none'
                item['currency'] = 'none'



        item['source'] = spider.name

        collection = self.mongo_base[spider.name]
        collection.insert_one(item)

        return item

