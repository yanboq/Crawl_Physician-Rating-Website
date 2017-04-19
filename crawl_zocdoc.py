# -*- coding: UTF-8 -*-
import html5lib, requests
import mysql.connector
import re, time
from bs4 import BeautifulSoup

# database info
username = 'root'
password = ''
host = 'localhost'
dbase = 'doctor_rating'



class crawlZocdoc:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password,host=host, database=dbase, buffered=True) # buffered = true for multiple cursors
        self.cursor = self.dbconn.cursor()

    def get_address(self):

        select_sql = (
        "SELECT cId,doctorNPI,searchResultUrl FROM doctor_rating.google_results2017 WHERE websiteName = 'www.zocdoc.com' AND crawlFlag = 0 LIMIT 1"
        )

        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():

            cId = cursor_data[0]
            doctor_NPI = cursor_data[1]
            doctor_url = cursor_data[2]

            print('---------New Url From Google----------')
            print(cId, doctor_NPI, doctor_url)

            url_string_doctor = doctor_url.split('/')[3]

            try:
                if url_string_doctor == "doctor" or url_string_doctor == 'professional':

                    url_last = doctor_url.split('/')[4]
                    try:
                        doctor_id = re.findall(r'\d+', url_last)[0]
                    except:
                        doctor_id = ""

                    if doctor_id:
                        self.get_data(doctor_id, doctor_url, doctor_NPI, cId)
                        insert_sql1 = (
                            "UPDATE `google_results2017` SET crawlFlag = 1 WHERE cId = %s")
                        insert_sql = (
                            "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName = 'www.zocdoc.com' and cId != %s and crawlflag = 0")
                        self.cursor.execute(insert_sql1, (cId,))
                        self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                        self.dbconn.commit()
                    else:

                        print('---------No information----------')

                else:
                    print('---------Fault url----------')

            except Exception as err:
                print("---------!!!Crawl Fail!!!----------")
                print(err)
                insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
                self.cursor.execute(insert_sql, (cId,))
                self.dbconn.commit()
                time.sleep(60)


    def get_data(self, doctor_id, doctor_url, doctor_NPI,cId):
        print("------------Doctor Information------------")

        wb_data = requests.get(doctor_url)

        time.sleep(1)

        soup = BeautifulSoup(wb_data.text,'html5lib')

        # soup = BeautifulSoup(open("C:/Users/Robin/Desktop/zocdoc.html"),"html5lib")

        doctor_name = soup.select(".sg-header3")[0].get_text().split(',')[0].strip()
        try:
            doctor_speciality = soup.select(".sg-header8")[0].get_text()
        except:
            doctor_speciality = ''

        # some doctors don't have reviews
        try:        
            rating_overall = soup.find('meta', itemprop="ratingValue")['content']
            rating_number = soup.find('meta', itemprop="reviewCount")['content']
            
        except Exception as err:
            rating_number = ""
            rating_overall = ""

        try:

            doctor_address_data = soup.select(".sg-para4.sg-navy")[0].get_text().strip().replace('\n','')

            doctor_address = ' '.join(doctor_address_data.split())
            doctor_city = doctor_address.split(',')[1].strip()
            doctor_state = doctor_address.split(',')[2].strip()
            doctor_zipcode = doctor_address.split(',')[3].strip()
        except Exception as err:

            doctor_address = ""
            doctor_city = ""
            doctor_state = ""
            doctor_zipcode = ""


        try:
            doctor_education_data = soup.select(".section-set")[0].get_text().strip().replace('\n','|')
            doctor_education = ' '.join(doctor_education_data.split())
        except Exception as err:
            doctor_education = ""

        try:
            doctor_language_data = soup.select(".section-set")[1].get_text().strip().replace('\n', '|')
            doctor_language = ' '.join(doctor_language_data.split())
        except Exception as err:
            doctor_education = ""

        try:
            doctor_statement = soup.find('p', itemprop="description").get_text().strip()
        except Exception as err:
            doctor_statement = ""

        # doctor information rows is dynamic
        doctor_board = ""
        doctor_membership = ""
        doctor_award = ""
        for i in range(2, 5):
            try:
                row_rawdata = soup.select(".section-set")[i].get_text().strip().replace('\n', '|')
                row_data = ' '.join(row_rawdata.split())
                row_name = soup.select(".sg-cool-grey")[i+1].get_text()
                if row_name == "Board Certifications":
                    doctor_board = row_data
                elif row_name == "Professional Memberships":
                    doctor_membership = row_data
                elif row_name == "Awards and Publications":
                    doctor_award = row_data
                else :
                    pass
            except Exception as err:
                pass




        doctor_zocdocaward = ""
        for i in soup.find_all('div', class_="js-badge"):
            doctor_zocdocaward = doctor_zocdocaward + "|" + i['data-title']

        try:
            print ("Doctor Information:", doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_city, doctor_state, doctor_zipcode)
            print ("Doctor Details:", doctor_education, doctor_language, doctor_board, doctor_membership, doctor_award, doctor_statement, doctor_zocdocaward)
        except Exception:
            pass

        if doctor_name != '':

            args = (doctor_id, doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_city, doctor_state, doctor_zipcode, doctor_education,
                    doctor_language, doctor_board, doctor_membership, doctor_award, doctor_statement, doctor_zocdocaward)
            add_doctor_result = ("INSERT IGNORE INTO zocdoc_doctor "
                                    "(doctor_id, doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, doctor_address, doctor_city, doctor_state, doctor_zipcode, doctor_education, doctor_language, doctor_board, doctor_membership, doctor_award, doctor_statement, doctor_zocdocaward) "
                                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s, %s, %s, %s, %s)")
            self.cursor.execute(add_doctor_result, args)
            self.dbconn.commit()

        if rating_number != "":
            insert_sql = ("UPDATE `google_results2017` SET ratingFlag = 1 WHERE cId = %s and websiteName = 'www.zocdoc.com'")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()

            try:

                for review_data in soup.select(".profile-review"):

                    rating_date = review_data.find('p', class_='sg-title').get_text()

                    if rating_date == "Review":
                        rating_date = ""

                    rating_three = review_data.find_all('div', class_="sg-rating-small")
                    rating_score = rating_three[0]['class'][1].split('-')[3].split('_')[0]
                    try:
                        rating_manner = rating_three[1]['class'][1].split('-')[3].split('_')[0]
                    except:
                        rating_manner = ''
                    try:
                        rating_waittime = rating_three[2]['class'][1].split('-')[3].split('_')[0]
                    except:
                        rating_waittime = ''

                    try:
                        rating_content = review_data.find('p', itemprop="reviewBody").get_text().strip()
                    except Exception as err:
                        rating_content = ""

                    try:
                        print (doctor_id, doctor_NPI, rating_date, rating_score, rating_manner, rating_waittime, rating_content)
                    except Exception:
                        pass

                    args = (doctor_id, doctor_NPI, rating_date, rating_overall, rating_manner, rating_waittime, rating_content)
                    add_rating_result = ("INSERT IGNORE INTO zocdoc_rating "
                                          "(doctor_id, doctor_NPI, rating_date, rating_score, rating_manner, rating_waittime, rating_content) "
                                          "VALUES (%s, %s, %s, %s, %s, %s, %s)")
                    self.cursor.execute(add_rating_result, args)
                    self.dbconn.commit()



            except Exception as err:
                print(err)

    def get_task_number(self):
        select_sql = (
            "SELECT count(*) FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName = 'www.zocdoc.com'"
        )
        self.cursor.execute(select_sql)
        for cursor_data in self.cursor.fetchall():
            task_number = int(cursor_data[0])
        print('The number of task this time: ', task_number)
        return task_number

    def main(self):
        task_number = self.get_task_number()
        for i in range(0, task_number):
            self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
            self.cursor = self.dbconn.cursor()
            self.get_address()
            self.cursor.close()
        self.dbconn.close()

if __name__ == '__main__':
    crawler = crawlZocdoc()
    crawler.main()