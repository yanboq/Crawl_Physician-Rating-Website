# -*- coding: UTF-8 -*-
import html5lib, requests
import mysql.connector
import re,time
from bs4 import BeautifulSoup


# database info
username = 'root'
password = ''
host = 'localhost'
dbase = 'doctor_rating'

class crawlVitals:

    def __init__(self):
        self.dbconn = mysql.connector.connect(user=username, password=password, host=host, database=dbase)
        self.cursor = self.dbconn.cursor()


    def get_address(self):

        select_sql = (
            "SELECT cId,doctorNPI,searchResultUrl,searchKeyword FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName LIKE '%vitals.com' LIMIT 1"
        )
        self.cursor.execute(select_sql)

        for cursor_data in self.cursor.fetchall():

            print('---------New Url From Google----------')

            cId = cursor_data[0]
            doctor_NPI = cursor_data[1]
            doctor_url = cursor_data[2]
            doctor_keyword = cursor_data[3]

            print(cId, doctor_NPI, doctor_url)

            try:
                url_category = doctor_url.split('/')[3]
                url_doctorid = doctor_url.split('/')[4]
                url_endstring = doctor_url.split('.')[-1]
                if url_endstring != 'html':
                    url_endstring = doctor_url.split('/')[-1]
            except IndexError:
                print('------------Fault url-------------')
                insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 2  WHERE cid = %s ")
                self.cursor.execute(insert_sql, (cId,))
                self.dbconn.commit()
            try:

                doctor_firstname = doctor_keyword.lower().split(' ')[0]
                name_regex = r"\b" + doctor_firstname + r"\b"
                url_validity = re.search(name_regex, doctor_url.replace('_', ' '), re.IGNORECASE)

                if url_category == "doctors" and url_endstring == 'html':
                    print('------------Valid url-------------')
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
                    self.get_data(doctor_url, doctor_NPI, cId)

                elif url_category == "doctors" and url_endstring != 'html':
                    print('------------Go to home page-------------')
                    doctor_url = 'http://alpha.vitals.com/doctors/' +doctor_url.split('/')[4] + '.html'
                    print('------------Valid url-------------')
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
                    self.get_data(doctor_url, doctor_NPI, cId)


                elif url_category == "podiatrists" and len(url_doctorid) == 6:
                    if url_endstring =='credentials' or url_endstring =='insurance':
                        print('------------Go to home page-------------')
                        url_doctorname = doctor_url.split('/')[5]
                        doctor_url = 'http://www.vitals.com/podiatrists/' + url_doctorid + '/' + url_doctorname

                        print('------------Valid url-------------')
                        insert_sql = (
                            "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                        insert_sql1 = (
                            "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                        self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                        self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                        self.dbconn.commit()
                        self.get_data(doctor_url, doctor_NPI, cId)

                    else:
                        print('------------Valid url-------------')
                        insert_sql = (
                            "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                        insert_sql1 = (
                            "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                        self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                        self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                        self.dbconn.commit()
                        self.get_data(doctor_url, doctor_NPI, cId)

                elif url_category == "chiropractors" and len(url_doctorid) == 6:
                    if url_endstring =='credentials' or url_endstring =='insurance':
                        print('------------Go to home page-------------')
                        url_doctorname = doctor_url.split('/')[5]
                        doctor_url = 'http://www.vitals.com/chiropractors/' + url_doctorid + '/' + url_doctorname
                        print('------------Valid url-------------')
                        insert_sql = (
                            "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                        insert_sql1 = (
                            "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                        self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                        self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                        self.dbconn.commit()
                        self.get_data(doctor_url, doctor_NPI, cId)
                    else:
                        print('------------Valid url-------------')
                        insert_sql = (
                            "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                        insert_sql1 = (
                            "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                        self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                        self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                        self.dbconn.commit()
                        self.get_data(doctor_url, doctor_NPI, cId)

                elif url_category == "dentists" and url_endstring[0:2] == 'Dr':
                    print('------------Valid url-------------')
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
                    self.get_data(doctor_url, doctor_NPI, cId)
                else:
                    print('------------Valid url-------------')
                    insert_sql = (
                        "UPDATE `google_results2017` SET crawlFlag = 5 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId != %s and crawlflag = 0")
                    insert_sql1 = (
                        "UPDATE `google_results2017` SET crawlFlag = 1 WHERE doctorNPI = %s and websiteName LIKE '%vitals.com' and cId = %s")
                    self.cursor.execute(insert_sql, (doctor_NPI, cId,))
                    self.cursor.execute(insert_sql1, (doctor_NPI, cId,))
                    self.dbconn.commit()
                    self.get_data(doctor_url, doctor_NPI, cId)

            except Exception as err:
                print("---------!!!Crawl Fail!!!----------")
                print(err)
                insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
                self.cursor.execute(insert_sql, (cId,))
                self.dbconn.commit()

                time.sleep(6)

    def get_data(self,doctor_url, doctor_NPI, cId):

        print("------------Doctor Information------------")

        wb_data = requests.get(doctor_url)
        soup = BeautifulSoup(wb_data.text, 'html5lib')

        url_category = doctor_url.split('/')[3]

        try:
            page_state = soup.select('.label.inactiveLabel')[0].get_text()
            print("---------!!!Crawl Fail!!!----------")
            insert_sql = ("UPDATE `google_results2017` SET crawlFlag = 4  WHERE cid = %s ")
            self.cursor.execute(insert_sql, (cId,))
            self.dbconn.commit()
        except:
            page_state = 'active'

        if page_state == 'active':

            name_temp = soup.select('.professionalHeader__name')[0].get_text().strip().split(' ')[:-1]
            doctor_name = ' '.join(name_temp)

            try:
                doctor_speciality = soup.select(".professionalHeader__specialty__type")[0].get_text().strip()
            except:
                doctor_speciality = ''


            try:
                rating_overall = soup.select('.number')[0].get_text().strip()
                rating_number = soup.find('span', itemprop="ratingCount").get_text()
            except:
                rating_overall = ''
                rating_number = ''
            try:
                rating_newscore = soup.select('p > strong')[0].get_text().strip()
            except:
                rating_newscore = ''
            try:
                review_number = soup.find('span', itemprop="reviewCount").get_text()
            except:
                review_number = ''

            try:
                doctor_street = soup.find('div', itemprop='streetAddress').get_text()
                doctor_city = soup.find('span', itemprop="addressLocality").get_text()
                doctor_state = soup.find('span', itemprop="addressRegion").get_text()
                doctor_zipcode = soup.find('span', itemprop="postalCode").get_text()
                doctor_address = doctor_street + ' ' + doctor_city + ' ' + doctor_state
            except:
                doctor_street = ''
                doctor_city = ''
                doctor_state = ''
                doctor_zipcode = ''
                doctor_address = ''

            try:
                print(doctor_name, doctor_speciality, rating_overall, rating_number, rating_newscore, doctor_street,
                      doctor_city, doctor_state, doctor_zipcode, doctor_address)
            except Exception:
                pass

            review_url = doctor_url.split('.html')[0] + '/reviews'
            wb_data = requests.get(review_url)
            soup = BeautifulSoup(wb_data.text, 'html5lib')

            if url_category == "chiropractors" or url_category == "podiatrists" or rating_number == '':
                doctor_appointments = ''
                doctor_promptness = ''
                doctor_staff = ''
                doctor_diagnosis = ''
                doctor_manner = ''
                doctor_spendstime = ''
                doctor_followup = ''
            else:
                doctor_appointments = self.get_starscore(0, 5, soup)
                doctor_promptness = self.get_starscore(5, 10, soup)
                doctor_staff = self.get_starscore(10, 15, soup)
                doctor_diagnosis = self.get_starscore(15, 20, soup)
                doctor_manner = self.get_starscore(20, 25, soup)
                doctor_spendstime = self.get_starscore(25, 30, soup)
                try:
                    doctor_followup = self.get_starscore(30, 35, soup)
                except:
                    doctor_followup = ''
                try:
                    print(doctor_appointments, doctor_promptness, doctor_staff, doctor_diagnosis, doctor_manner,
                          doctor_spendstime, doctor_followup)
                except Exception:
                    pass

            args = (doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, rating_newscore, doctor_street, doctor_city, doctor_state, doctor_zipcode, doctor_address, doctor_appointments, doctor_promptness, doctor_staff, doctor_diagnosis, doctor_manner, doctor_spendstime, doctor_followup)
            add_doctor_result = ("INSERT IGNORE INTO vitals_doctor "
                                 "(doctor_NPI, doctor_name, doctor_speciality, rating_overall, rating_number, rating_newscore, doctor_street, doctor_city, doctor_state, doctor_zipcode, doctor_address, doctor_appointments, doctor_promptness, doctor_staff, doctor_diagnosis, doctor_manner, doctor_spendstime, doctor_followup) "
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self.cursor.execute(add_doctor_result, args)
            self.dbconn.commit()

            if rating_number != '':
                insert_sql = (
                "UPDATE `google_results2017` SET ratingflag = 1 WHERE cId = %s and websiteName LIKE '%vitals.com'")
                self.cursor.execute(insert_sql, (cId,))
                self.dbconn.commit()

            if rating_number != '' and review_number != '':

                page_number = int(review_number) // 12 + 1
                for i in range(0, page_number):
                    page_url = "?page=" + str(i)
                    doctor_page_url = review_url + page_url
                    print(doctor_page_url)

                    wb_data = requests.get(doctor_page_url)
                    soup = BeautifulSoup(wb_data.text, 'html5lib')

                    for review_all_data in soup.select('.individualReviews'):

                        print("------------Review------------")

                        rating_score = review_all_data.select('.number')[0].get_text().split(' ')[0]
                        rating_date = review_all_data.select('.patientReviews__date')[0].get_text()

                        rating_appointments = ''
                        rating_promptness = ''
                        rating_staff = ''
                        rating_diagnosis = ''
                        rating_spendstime = ''
                        rating_followup = ''
                        for rating_row_data in review_all_data.select('.patientReviews__ratingQuestion'):
                            ratingname = rating_row_data.select('.question')[0].get_text().strip()
                            # rating_row_data = rating_row_data.select('.answer')
                            # print( rating_row_data)
                            if ratingname == "Easy Appointment":
                                rating_appointments = self.get_review_starscore(0, 5, rating_row_data)
                            elif ratingname == "Promptness":
                                rating_promptness = self.get_review_starscore(0, 5, rating_row_data)
                            elif ratingname == "Friendly Staff":
                                rating_staff = self.get_review_starscore(0, 5, rating_row_data)
                            elif ratingname == "Fair and Accurate Diagnosis":
                                rating_diagnosis = self.get_review_starscore(0, 5, rating_row_data)
                            elif ratingname == "Spends Time with Patients":
                                rating_spendstime = self.get_review_starscore(0, 5, rating_row_data)
                            elif ratingname == "Appropriate Follow-up":
                                rating_followup = self.get_review_starscore(0, 5, rating_row_data)

                        rating_content = review_all_data.select('.patientReviews__reviewComments')[0].get_text().strip()

                        try:
                            print(doctor_NPI, rating_score, rating_date, rating_appointments, rating_promptness,
                                  rating_staff, rating_diagnosis, rating_spendstime, rating_followup, rating_content)
                        except Exception:
                            pass
                        args = (
                        doctor_NPI, rating_score, rating_date, rating_appointments, rating_promptness, rating_staff,
                        rating_diagnosis, rating_spendstime, rating_followup, rating_content)
                        add_rating_result = ("INSERT IGNORE INTO vitals_rating "
                                             "(doctor_NPI, rating_score, rating_date, rating_appointments, rating_promptness, rating_staff, rating_diagnosis, rating_spendstime, rating_followup, rating_content) "
                                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                        self.cursor.execute(add_rating_result, args)
                        self.dbconn.commit()
            else:
                pass



    def get_starscore(self, begin, end, soup):
        detail_score = 0
        for i in range(begin, end):
            try:
                rating_classname = soup.select('span.questionValue > i')[i]['class']
            except:
                try:
                    rating_classname = soup.select('span > i')[i]['class']
                except:
                    detail_score = ''
                    continue

            if rating_classname[0] == 'icons-star':
                detail_score += 1
            elif rating_classname[0] == 'icons-star-half':
                detail_score += 0.5
            try:
                rating_classname[1] == 'empty'
                detail_score -= 1
            except:
                pass

        return detail_score


    def get_review_starscore(self, begin, end, soup):
        detail_score = 0
        for i in range(begin, end):
            try:
                rating_classname = soup.select('span > i')[i]['class']

                if rating_classname[0] == 'icons-star':
                    detail_score += 1
                elif rating_classname[0] == 'icons-star-half':
                    detail_score += 0.5
                try:
                    rating_classname[1] == 'empty'
                    detail_score -= 1
                except:
                    pass
            except:
                pass

        return detail_score


    def get_task_number(self):
        select_sql = (
            "SELECT count(*) FROM doctor_rating.google_results2017 WHERE crawlFlag = 0 AND websiteName LIKE '%vitals.com'"
        )
        self.cursor.execute(select_sql)
        for cursor_data in self.cursor.fetchall():
            task_number = int(cursor_data[0])
        print('The number of task in this time: ',task_number )
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
    crawler = crawlVitals()
    crawler.main()