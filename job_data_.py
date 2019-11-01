import datetime as dt
from bs4 import BeautifulSoup
import requests
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float
import sys

def progress(count, total, suffix=''):
    bar_len = 40
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '█' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %s%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()

Base = declarative_base()


class Jobs(Base):
    __tablename__ = 'jobs_'
    id = Column(Integer, primary_key=True)
    j_id = Column(Integer)
    province = Column(String)
    region = Column(String)
    specialization = Column(String)
    enterprise = Column(String)
    inn = Column(Integer)
    salary = Column(Integer)
    rate = Column(Float)
    free_places = Column(Integer)
    published = Column(Date)

    def __repr__(self):
        return "<Jobs(j_id='{}', province='{}', rate='{}',\
         salary={}, published={})>"\
            .format(self.j_id, self.province, self.rate,
                    self.salary, self.published)


class Database:
    auth = 'postgres:parol123@localhost:5432'
    DATABASE_URI = 'postgres+psycopg2://{}/job_data'.format(auth)
    engine = create_engine(DATABASE_URI)

    def __init__(self):
        self.connection = self.engine.connect()
        print("db instance created", dt.datetime.now())

    def create_table(self):
        Base.metadata.create_all(self.engine)

    def session_ctrl(self, open=False, close=False):
        """ control session """
        if open:
            self.session = Session(bind=self.connection)
        elif close:
            self.session.close()

    def save(self, data):
        jdata = Jobs(
            j_id=data[0],
            province=data[1],
            region=data[2],
            specialization=data[3],
            enterprise=data[4],
            inn=data[5],
            salary=data[6],
            rate=data[7],
            free_places=data[8],
            published=data[9]
        )
        self.session.add(jdata)
        self.session.commit()


class PageData:
    def __init__(self, url, start_page_num, end_page_num):
        self.url_def = url
        self.page_num = end_page_num
        self.start_page_num = start_page_num
        self.collect_site_data()

    def variables_cost(self, list):
        """ defining variable types """

        list[0] = int(list[0])
        list[5] = int(list[5])
        sl = list[6].split(',')[0].split('.')
        salary = ''

        for i in sl:
            salary += i

        list[6] = int(salary)
        list[7] = float(list[7])
        list[8] = int(list[8])
        list[9] = dt.datetime.strptime(list[9], '%d.%m.%Y').date()
        return list

    def collect_page_data(self, content=None):
        """ getting full data from one page """

        bhtml = BeautifulSoup(content, 'html.parser')
        tds = bhtml.find('tbody')
        items = tds.find_all("tr")

        self.data_lists = []

        for tr in items:
            sub_list = []
            for td in tr.find_all("td"):
                sub_list.append(td.text)
            del sub_list[-1]
            sub_list = self.variables_cost(sub_list)
            db.save(sub_list)

    def collect_site_data(self):
        """ collecting all of site data """

        for page in range(self.start_page_num, self.page_num+1):
            url = self.url_def + "?dp-1-page={}".format(page)
            content = get(url, stream=True).content
            self.collect_page_data(content=content)
            progress(page-1, self.page_num+1, 'collecting')

        print("\nIn total: {} case data were collected.\
            \n{}".format(20*self.page_num, dt.datetime.now()))


url = "http://ish.mehnat.uz/vacancy/index"
db = Database()  # connecting to database
db.create_table()  # creating table
db.session_ctrl(open=True)  # creating session
PageData(start_page_num=1, end_page_num=1672, url=url)
db.session_ctrl(close=True) # closing session
