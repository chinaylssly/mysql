#_*_ coding:utf-8 _*_

from MySQL import MySQL
from config import host,user,password,database
import logging


if __name__=='__main__':

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                        # filename=log_file,
                        # filemode='a'
                        ) 

class mysql(MySQL):

    def __init__(self,host=host,user=user,password=password,db=''):
        super(mysql,self).__init__(host=host,user=user,password=password,db=database)

    def create_database(self,):

        query='create database if not exists %s'%database
        self.execute(query=query)


    def create_table_category(self,):

        query='''create table if not exists %s.category(
                category varchar(20),
                url varchar(100),
                status int not null default '0',
                create_time timestamp default current_timestamp
                )default charset utf8'''%(database)

        self.execute(query=query)


    def check_table_category(self,):

        query='select category,url from category where status=0'

        return self.execute(query=query)



    def insert_table_category(self,category,url):

        query='insert ignore into category (category,url) values("%s","%s")'%(category,url)
        self.execute(query=query)


    def update_table_category_status(self,url,status=1):

        query='update category set status="%s" where url="%s" '%(status,url)
        self.execute(query=query)




    def create_table_book(self,):

        query='''create table if not exists %s.book(
                url varchar(100) not null primary key,
                book varchar(50) not null,
                category varchar(20),
                refer varchar(100) not null,
                status int not null default '0',
                create_time timestamp default current_timestamp
                )default charset utf8'''%(database)

        self.execute(query=query)


    def check_table_book(self,):

        query='select url,book,category from book where status=0 order by category'

        return self.execute(query=query)



    def insert_table_book(self,url,book,refer,category):

        query='insert ignore into book (url,book,refer,category) values("%s","%s","%s","%s")'%(url,book,refer,category)

        self.execute(query=query)


    def update_table_book_status(self,url,status=1):

        query='update book set status="%s" where url="%s"'%(status,url)
        self.execute(query=query)



    def create_table_item(self,):

        query='''create table if not exists %s.item(
                url varchar(100) not null primary key,
                item varchar(50)not null,
                book varchar(50),
                category varchar(20),
                refer varchar(100) not null,
                status int not null default '0',
                create_time timestamp default current_timestamp
                )default charset utf8'''%(database)

        self.execute(query=query)




    def check_table_item(self,):

        query=u'select item,url,book,category from item where status=0 and category="玄幻" order by create_time asc limit 1000'

        return self.execute(query=query)


    def insert_table_item(self,url,item,book,category,refer):

        query='insert ignore into item (url,item,book,category,refer) values("%s","%s","%s","%s","%s")'%(url,item,book,category,refer)

        self.execute(query=query)

    def update_table_item_status(self,url,status=1):

        query='update item set status="%s" where refer="%s"'%(status,url)
        self.execute(query=query)

    def check_table_item_status_by_floder(self,category,book,item):

        query=u'select category,book,item from item where category="%s" and book="%s" and item=%s'%(category,book,item)
        return self.execute(query)

    def update_table_item_status_by_path(self,category,book,item,status=1):

        query=u'update item set status="%s"  where category="%s" and book="%s" and item=%s'%(status,category,book,item)
        self.execute(query)


    def truncate_table(self,tb):

        query='truncate table %s'%(tb)
        self.execute(query=query)

    def drop_table(self,tb):

        query='drop table %s'%(tb)
        self.execute(query=query)

   




def test():

    sql=mysql(db=database)
    sql.create_database()
    sql.create_table_category()
    sql.create_table_book()
    sql.create_table_item()

   


if __name__=='__main__':

    test()

    pass




