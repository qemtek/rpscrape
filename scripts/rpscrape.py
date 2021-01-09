#!/usr/bin/env python3

""" Scrapes results and saves them in csv format """

from datetime import date, timedelta, datetime
from git import Repo, cmd
import json
from lxml import html
import os
from re import search
import requests
import sys
from time import sleep


class Completer:
    def __init__(self, options):
        self.options = sorted(options)

    def complete(self, text, state):
        if state == 0:
            if text:
                self.matches = [s for s in self.options if s and s.startswith(text)]
            else:
                self.matches = self.options[:]
        try:
            return self.matches[state]
        except IndexError:
            return None


def options(opt="help"):
    opts = "\n".join(
        [
            "       regions              List all available region codes",
            "       regions [search]     Search for specific country code",
            "",
            "       courses              List all courses",
            "       courses [search]     Search for specific course",
            "       courses [region]     List courses in region - e.g courses ire",
            "",
            "       -d, date             Scrape race by date - e.g -d 2019/12/17 gb",
        ]
    )

    if opt == "help" or opt == "?":
        print(
            "\n".join(
                [
                    "  Usage:",
                    "       ~$ ./RPScraper.py"
                    "       [RPScraper]> [region|course] [year|range] [flat|jumps]",
                    "",
                    "       Regions have alphabetic codes.",
                    "       Courses have numeric codes.",
                    "",
                    "  Examples:",
                    "       [RPScraper]> ire 1999 flat",
                    "       [RPScraper]> gb 2015-2018 jumps",
                    "       [RPScraper]> 533 1998-2018 flat",
                    "",
                    "  Options:",
                    "{}".format(opts),
                    "",
                    "  More info:",
                    "       help            Show help",
                    "       options         Show options",
                    "       cls, clear      Clear screen",
                    "       q, quit, exit   Quit",
                    "",
                ]
            )
        )
    else:
        print(opts)


def courses(code='all'):
    with open(f'../courses/{code}_course_ids', 'r') as courses:
        for course in courses:
            yield (course.split('-')[0].strip(), ' '.join(course.split('-')[1::]).strip())
         

def course_name(code):
    if code.isalpha():
        return code
    for course in courses():
        if course[0] == code:
            return course[1].replace('()', '').replace(' ', '-')


def course_search(term):
    for course in courses():
        if term.lower() in course[1].lower():
            print_course(course[0], course[1])


def print_course(code, course):
    if len(code) == 5:
        print(f'     CODE: {code}| {course}')
    elif len(code) == 4:
        print(f'     CODE: {code} | {course}')
    elif len(code) == 3:
        print(f'     CODE: {code}  | {course}')
    elif len(code) == 2:
        print(f'     CODE: {code}   | {course}')
    else:
        print(f'     CODE: {code}    | {course}')


def print_courses(code='all'):
    for course in courses(code):
        print_course(course[0], course[1])


def valid_course(code):
    return code in [course[0] for course in courses()]


def x_y():
    from base64 import b64decode
    return b64decode(
        'aHR0cHM6Ly93d3cucmFjaW5ncG9zdC5jb206NDQzL3Byb2ZpbGUvY291cnNlL2ZpbHRlci9yZXN1bHRz'
    ).decode('utf-8'), b64decode('aHR0cHM6Ly93d3cucmFjaW5ncG9zdC5jb20vcmVzdWx0cw==').decode('utf-8')


def regions():
    with open('../courses/_countries', 'r') as regions:
        return json.load(regions)


def region_search(term):
    for code, region in regions().items():
        if term.lower() in region.lower():
            print_region(code, region)


def print_region(code, region):
    if len(code) == 3:
        print(f'     CODE: {code} | {region}')
    else:
        print(f'     CODE: {code}  | {region}')


def print_regions():
    for code, region in regions().items():
        print_region(code, region)


def valid_region(code):
    return code in regions().keys()


def valid_years(years):
    if years:
        return all(year.isdigit() and int(year) >= 1987 and int(year) <= int(datetime.today().year) for year in years)

    return False


def valid_date(date):
    if len(date.split('/')) == 3:
        try:
            year, month, day = [int(x) for x in date.split('/')]
            return year >= 1987 and year <= int(datetime.today().year) and month > 0 and month <= 12 and day > 0 and day <= 31
        except ValueError:
            return False

    return False


def check_date(date):
    if '-' in date and len(date.split('-')) < 3:
        return valid_date(date.split('-')[0]) and valid_date(date.split('-')[1])

    return valid_date(date)


def fraction_to_decimal(fractions):
    decimal = []
    for fraction in fractions:
        if fraction == '' or fraction == 'No Odds':
            decimal.append('')
        elif 'evens' in fraction.lower() or fraction.lower() == 'evs':
            decimal.append('2.00')
        else:
            decimal.append('{0:.2f}'.format(float(fraction.split('/')[0]) / float(fraction.split('/')[1]) + 1.00))

    return decimal


def convert_date(date):
    dmy = date.split('-')
    
    return dmy[0] + '-' + dmy[1] + '-' + dmy[2]


def pedigree_info(pedigrees):
    sires, dams, damsires = [], [], []

    for p in pedigrees:
        ped_info = p.findall('a')

        if len(ped_info) > 0:
            sire = ped_info[0].text.strip()

            if '(' in sire:
                sire = sire.split('(')[0].strip() + ' (' + sire.split('(')[1]
            else:
                sire = sire + ' (GB)'

            sires.append(sire)
        else:
            sires.append('')

        if len(ped_info) > 1:
            dam = ped_info[1].text.strip()
            dam_nat = ped_info[1].find('span').text

            if dam_nat is not None:
                dam = dam + ' ' + dam_nat.strip()
            else:
                dam = dam + ' (GB)'
        
            dams.append(dam)
        else:
            dams.append('')

        if len(ped_info) > 2:
            damsire = ped_info[2].text.strip().strip('()')
            if damsire == 'Damsire Unregistered':
                damsire = ''
            damsires.append(damsire)
        else:
            damsires.append('')

    return sires, dams, damsires


def class_from_rating_band(rating_band, code):
    try:
        upper = int(rating_band.split('-')[1])
    except:
        return ''

    if code == 'flat':
        if upper >= 100:
            return 'Class 2'
        if upper >= 90:
            return 'Class 3'
        if upper >= 80:
            return 'Class 4'
        if upper >= 70:
            return 'Class 5'
        if upper >= 60:
            return 'Class 6'
        if upper >= 40:
            return 'Class 7'
    else:
        if upper >= 140:
            return 'Class 2'
        if upper >= 120:
            return 'Class 3'
        if upper >= 100:
            return 'Class 4'
        if upper >= 85:
            return 'Class 5'

    return ''


def clean_race_name(race):
    if 'Class' in race:
        if 'Class A' in race or 'Class 1' in race:
            return race.replace('Class A', '').replace('Class 1', '').replace('()', '')
        if 'Class B' in race or 'Class 2' in race:
            return race.replace('Class B', '').replace('Class 2', '').replace('()', '')
        if 'Class C' in race or 'Class 3' in race:
            return race.replace('Class C', '').replace('Class 3', '').replace('()', '')
        if 'Class D' in race or 'Class 4' in race:
            return race.replace('Class D', '').replace('Class 4', '').replace('()', '')
        if 'Class E' in race or 'Class 5' in race:
            return race.replace('Class E', '').replace('Class 5', '').replace('()', '')
        if 'Class F' in race or 'Class 6' in race:
            return race.replace('Class F', '').replace('Class 6', '').replace('()', '')
        if 'Class H' in race or 'Class 7' in race:
            return race.replace('Class H', '').replace('Class 7', '').replace('()', '')
        if 'Class G' in race:
            return race.replace('Class G', '').replace('()', '')
        if 'Trusthouse Forte Mile Guaranteed Minimum Value £60000 (Group' in race:
            return race.replace('(Group', '')
    if 'Group' in race:
        if 'Group 1' in race or 'Grade 1' in race:
            return race.replace('Group 1', '').replace('Grade 1', '').replace('()', '')
        if 'Group 2' in race or 'Grade 2' in race:
            return race.replace('Group 2', '').replace('Grade 2', '').replace('()', '')
        if 'Group 3' in race or 'Grade 3' in race:
            return race.replace('Group 3', '').replace('Grade 3', '').replace('()', '')

    return race


def try_get_class(race):
    if 'Class A' in race or 'Class 1' in race:
        return 'Class 1'
    if 'Class B' in race or 'Class 2' in race:
        return 'Class 2'
    if 'Class C' in race or 'Class 3' in race:
        return 'Class 3'
    if 'Class D' in race or 'Class 4' in race:
        return 'Class 4'
    if 'Class E' in race or 'Class 5' in race:
        return 'Class 5'
    if 'Class F' in race or 'Class 6' in race:
        return 'Class 6'
    if 'Class H' in race or 'Class 7' in race:
        return 'Class 7'
    if 'Class G' in race:
        return 'Class 6'
    if '(premier handicap)' in race:
        return 'Class 2'

    return ''


def try_get_pattern(race, race_class):
    pattern = ''
    r_class = 'Class 1'

    if 'Forte Mile' in race and '(Group' in race:
        return r_class, 'Group 2'

    if '(Group' in race:
        try:
            pattern = search('(\(Grou..)\w+', race).group(0).strip('(')
        except AttributeError:
            pattern = search('(\(Grou.)\w+', race).group(0).strip('(')
        return r_class, pattern
    if '(Grade' in race:
        try:
            pattern = search('(\(Grad..)\w+', race).group(0).strip('(')
        except AttributeError:
            pattern = search('(\(Grad.)\w+', race).group(0).strip('(')
        return r_class, pattern
    if 'Grade' in race:
        return r_class, search('Grad..\w+', race).group(0)
    if '(Local Group 1)' in race:
        return r_class, 'Group 1'
    if '(Local Group 2)' in race:
        return r_class, 'Group 2'
    if '(Local Group 3)' in race:
        return r_class, 'Group 3'
    if '(Listed' in race:
        return r_class, 'Listed'

    return race_class, pattern


def try_get_race_type(race, race_dist):
    if race_dist >= 12:
        if 'national hunt flat' in race or 'nh flat race' in race or 'mares flat race' in race:
            return 'NH Flat'
        if 'inh bumper' in race or ' sales bumper' in race or 'kepak flat race' in race:
            return 'NH Flat'

    if race_dist >= 15:
        if ' hurdle' in race:
            return 'Hurdle'
        if ' chase' in race:
            return 'Chase'

    return ''


def sex_restricted(race):
    if ('(Entire Colts & Fillies)') in race or '(Colts & Fillies)' in race:
        return 'C & F'
    elif '(Fillies & Mares)' in race or '(Filles & Mares)' in race:
        return 'F & M'
    elif '(Fillies)' in race or 'Fillies' in race:
        return 'F'
    elif '(Colts & Geldings)' in race or '(C & G)' in race or ' Colts & Geldings)' in race:
        return 'C & G'
    elif '(Mares & Geldings)' in race:
        return 'M & G'
    elif 'Mares' in race:
        return 'M'
    else:
        return ''


def distance_to_furlongs(distance):
    dist = ''.join([d.strip().replace('¼', '.25').replace('½', '.5').replace('¾', '.75') for d in distance])

    if 'm' in dist:
        if len(dist) > 2:
            dist = int(dist.split('m')[0]) * 8 + float(dist.split('m')[1].strip('f'))
        else:
            dist = int(dist.split('m')[0]) * 8
    else:
        dist = dist.strip('f')

    return float(dist)


def distance_to_metres(distance):
    dist = distance.lower()
    metres = 0

    if 'm' in dist:
        metres += int(dist.split('m')[0]) * 1609.34

    if 'f' in dist:
        metres += int(dist.split('f')[0][-1]) * 201.168

    if 'yds' in dist:
        if 'f' in dist:
            metres += int(dist.split('f')[1].strip('yds')) * .914
        elif 'm' in dist:
            metres += int(dist.split('m')[1].strip('yds')) * .914

    return round(metres)


def get_races(tracks, names, years, code, xy):
    races = []
    for track, name in zip(tracks, names):
        for year in years:
            r = requests.get(f'{xy[0]}/{track}/{year}/{code}/all-races', headers={'User-Agent': 'Mozilla/5.0'})
            if r.status_code == 200:
                try:
                    results = r.json()
                    if results['data']['principleRaceResults'] == None:
                        print(f'No {code} race data for {course_name(track)} in {year}.')
                    else:
                        for result in results['data']['principleRaceResults']:
                            races.append(f'{xy[1]}/{track}/{name}/{result["raceDatetime"][:10]}/{result["raceInstanceUid"]}')
                except:
                    pass
            else:
                print(f'Unable to access races from {course_name(track)} in {year}')
    return races


def get_race_links(date, region):
    valid_courses = [x.split('-')[0].strip() for x in open(f'../courses/{region}_course_ids')]

    r = requests.get(
        f'https://www.racingpost.com/results/{date}', headers={'User-Agent': 'Mozilla/5.0'}
    )

    while r.status_code == 403:
        sleep(5)

        r = requests.get(
            f'https://www.racingpost.com/results/{date}', headers={'User-Agent': 'Mozilla/5.0'}
        )

    doc = html.fromstring(r.content)
    race_links = doc.xpath('//a[@data-test-selector="link-listCourseNameLink"]')

    links = []

    for race in race_links:
        if 'https://www.racingpost.com' + race.attrib['href'] not in links:
            if race.attrib['href'].split('/')[2] in valid_courses:
                links.append('https://www.racingpost.com' + race.attrib['href'])

    return links


def calculate_times(win_time, dist_btn, going, code, course):
    times = []

    if code == 'flat':
        if 'Firm' in going or 'Standard' in going or 'Fast' in going or 'Hard' in going or 'Slow' in going or 'Sloppy':
            if 'southwell' in course.lower():
                lps_scale = 5
            else:
                lps_scale = 6
        elif 'Good' in going:
            if 'Soft' in going or 'Yielding'in going:
                lps_scale = 5.5
            else:
                lps_scale = 6
        elif 'Soft' in going or 'Heavy' in going or 'Yielding' in going:
            lps_scale = 5
    else:
        if 'Firm' in going or 'Standard' in going:
            if 'southwell' in course.lower():
                lps_scale = 4
            else:
                lps_scale = 5
        elif 'Good' in going:
            if 'Soft' in going or 'Yielding'in going:
                lps_scale = 4.5
            else:
                lps_scale = 5
        elif 'Soft' in going or 'Heavy' in going or 'Yielding' in going or 'Slow' in going:
            lps_scale = 4

    for dist in dist_btn:
        try:
            time = (win_time + (float(dist) / lps_scale))
            times.append('{}:{:2.2f}'.format(int(time // 60), time % 60))
        except ValueError:
            times.append('')

    return times


def clean(data):
    return [d.strip().replace('–', '') for d in data]


def scrape_races(races, target, years, code):
    if not os.path.exists(f'../data/{code}/{target.lower()}/'):
        os.makedirs(f'../data/{code}/{target.lower()}/')

    with open(f'../data/{code}/{target.lower()}/{years}.csv', 'w', encoding="utf-8") as csv:

        csv.write(
            'Date,Course,Off,Name,Type,Class,Pattern,Rating_Band,Age_Band,Sex_Rest,Dist,Dist_Y,Dist_M,Dist_F,'
            'Going,Num,Pos,Ran,Draw,Btn,Ovr_Btn,Horse,SP,Dec,Age,Sex,Wgt,Lbs,HG,Time,Jockey,Trainer,OR,RPR,TS,'
            'Prize,Sire,Dam,Damsire,Owner,Comment\n'
        )

        for race in races:
            r = requests.get(race, headers={'User-Agent': 'Mozilla/5.0'})

            while r.status_code == 403 or r.status_code == 404 or r.status_code == 503:
                if r.status_code == 404:
                    sleep(2)
                elif r.status_code == 503:
                    sleep(2)
                else:
                    sleep(5)

                r = requests.get(race, headers={'User-Agent': 'Mozilla/5.0'})

            doc = html.fromstring(r.content)

            course = race.split('/')[5]
            date = convert_date(race.split('/')[6])

            try:
                r_time = doc.xpath("//span[@data-test-selector='text-raceTime']/text()")[0]
            except IndexError:
                r_time = ''

            try:
                race_name = doc.xpath("//h2[@class='rp-raceTimeCourseName__title']/text()")[0].strip().strip('\n')\
                    .replace(',', ' ').replace('"', '').replace('\x80', '').replace('\\x80', '').replace('  ', ' ')
            except IndexError:
                race_name = ''

            try:
                race_class = doc.xpath("//span[@class='rp-raceTimeCourseName_class']/text()")[0].strip().strip('()')
            except:
                race_class = ''

            if race_class == '':
                race_class = try_get_class(race_name)

            race_name = clean_race_name(race_name)

            try:
                race_class, pattern = try_get_pattern(race_name, race_class)
            except AttributeError:
                print('try_get_pattern error:')
                print('Race link: ', race)
                print('Race name: ', race_name)
                sys.exit()

            try:
                band = doc.xpath("//span[@class='rp-raceTimeCourseName_ratingBandAndAgesAllowed']/text()")[0].strip().strip('()')
            except:
                band = ''

            rating_band = ''
            age_band = ''

            if len(band.split(',')) > 1:
                for x in band.split(','):
                    if 'yo' in x:
                        age_band = x.strip()
                    elif '-' in x:
                        rating_band = x.strip()
            else:
                if 'yo' in band:
                    age_band = band.strip()
                elif '-' in band:
                    rating_band = band.strip()

            if race_class == '' and rating_band != '':
                race_class = class_from_rating_band(rating_band, code)

            sex_rest = sex_restricted(race_name)

            try:
                distance = doc.xpath("//span[@data-test-selector='block-distanceInd']/text()")[0].strip()
            except IndexError:
                distance = ''

            try:
                dist_y = doc.xpath("//span[@data-test-selector='block-fullDistanceInd']/text()")[0].strip().strip('()')
            except IndexError:
                dist_y = ''

            try:
                dist_f = distance_to_furlongs(distance)
            except ValueError:
                print('distance_to_furlongs()')
                print('Distance: ', distance)
                print('Race: ', race)
                sys.exit()

            dist_m = distance_to_metres(dist_y)
            
            if dist_m == 0:
                dist_m = round(dist_f * 201.168)
            
            dist_y = round(dist_m * 1.09361)
            dist_f = str(dist_f).replace('.0', '') + 'f'

            try:
                going = doc.xpath("//span[@class='rp-raceTimeCourseName_condition']/text()")[0].strip()
            except IndexError:
                going = ''

            race_type = ''

            if code == 'flat' and 'national hunt flat' not in race_name.lower():
                race_type = 'Flat'
            else:
                try:
                    if 'hurdle' in doc.xpath("//span[@class='rp-raceTimeCourseName_hurdles']/text()")[0].lower():
                        race_type = 'Hurdle'
                    elif 'fence' in doc.xpath("//span[@class='rp-raceTimeCourseName_hurdles']/text()")[0].lower():
                        race_type = 'Chase'
                except IndexError:
                    race_type = try_get_race_type(race_name.lower(), float(dist_f.strip('f')))

            if race_type == '':
                try_get_race_type(race_name.lower(), float(dist_f.strip('f')))

            if race_type == '':
                race_type = 'Flat'

            pedigrees = doc.xpath("//tr[@data-test-selector='block-pedigreeInfoFullResults']/td")
            sires, dams, damsires = pedigree_info(pedigrees)

            sex = []

            for x in pedigrees:
                try:
                    sex.append(x.text.strip().split()[1].upper())
                except IndexError:
                    sex.append(x.text.strip().upper())

            coms = doc.xpath("//tr[@class='rp-horseTable__commentRow ng-cloak']/td/text()")
            coms = [x.strip().replace('  ', '').replace(',', ' -').replace('\n', ' ').replace('\r', '') for x in coms]
            
            possy = doc.xpath("//span[@data-test-selector='text-horsePosition']/text()")
            del possy[1::2]
            pos = [x.strip() for x in possy]
            
            prizes = doc.xpath("//div[@data-test-selector='text-prizeMoney']/text()")
            prize = [p.strip().replace(",", '').replace('£', '') for p in prizes]
            try:
                del prize[0]
                for i in range(len(pos) - len(prize)):
                    prize.append('')
            except IndexError:
                prize = ['' for x in range(len(pos))]

            draw = clean(doc.xpath("//sup[@class='rp-horseTable__pos__draw']/text()"))
            draw = [d.strip("()") for d in draw]

            btn = []
            ovr_btn = []

            for x in doc.xpath("//span[@class='rp-horseTable__pos__length']"):

                distances = x.findall('span')

                if len(distances) == 2:
                    if distances[0].text == None:
                        btn.append('0')
                    else:
                        btn.append(distances[0].text)
                    if distances[1].text == None:
                        ovr_btn.append('0')
                    else:
                        ovr_btn.append(distances[1].text.strip('[]'))
                else:
                    if distances[0].text == None:
                        btn.append('0')
                        ovr_btn.append('0')
                    else:
                        if distances[0].text == 'dht':
                            btn.append(distances[0].text)
                            try:
                                ovr_btn.append(ovr_btn[-1])
                            except IndexError:
                                ovr_btn.append(btn[-1])
                        else:
                            btn.append(distances[0].text)
                            ovr_btn.append(distances[0].text)

            try:
                btn = [
                    b.strip().replace('¼', '.25').replace('½', '.5').replace('¾', '.75').replace('snk', '0.2').replace('nk', '0.3')\
                    .replace('sht-hd', '0.1').replace('shd', '0.1').replace('hd', '0.2').replace('nse', '0.05').replace('dht', '0').replace('dist', '30')\
                    for b in btn
                ]
            except AttributeError:
                print("btn error: ", race)
                sys.exit()

            ovr_btn = [
                b.strip().strip("[]").replace('¼', '.25').replace('½', '.5').replace('¾', '.75').replace('snk', '0.2').replace('nk', '0.3')\
                .replace('sht-hd', '0.1').replace('shd', '0.1').replace('hd', '0.2').replace('nse', '0.05').replace('dht', '0').replace('dist', '30')\
                for b in ovr_btn
            ]

            if len(ovr_btn) < len(pos):
                ovr_btn.extend(['' for x in range(len(pos) - len(ovr_btn))])

            if len(btn) < len(pos):
                btn.extend(['' for x in range(len(pos) - len(btn))])

            time_btn = []

            for x, y in zip(btn, ovr_btn):
                try:
                    if float(x) < .25:
                        time_btn.append(str(float(x) + float(y)))
                    else:
                        time_btn.append(y)
                except ValueError:
                    time_btn.append(y)

            numbers = [x.strip('.') for x in doc.xpath("//span[@class='rp-horseTable__saddleClothNo']/text()")]

            try:
                ran = doc.xpath(
                    "//span[@class='rp-raceInfo__value rp-raceInfo__value_black']/text()"
                )[0].replace('ran', '').strip('\n').strip()
            except IndexError:
                print(r.status_code)
                print(race)
                print('Failed to find number of runners.')

                with open('ran_error.html', 'w') as ran_error:
                    ran_error.write(str(r.content))

                sys.exit()

            horse_nat = doc.xpath("//span[@class='rp-horseTable__horse__country']/text()")
            nats = []
            for nat in horse_nat:
                if nat.strip() == '':
                    nats.append('(GB)')
                else:
                    nats.append(nat.strip())

            name = clean(doc.xpath("//a[@data-test-selector='link-horseName']/text()"))

            sps = clean(doc.xpath("//span[@class='rp-horseTable__horse__price']/text()"))
            sps = [x.replace('No Odds', '') for x in sps]

            jock = clean(doc.xpath("//a[@data-test-selector='link-jockeyName']/text()"))
            del jock[::2]

            trainer = clean(doc.xpath("//a[@data-test-selector='link-trainerName']/text()"))
            del trainer[1::2]
            del trainer[1::2]

            owners = doc.xpath("//a[@data-test-selector='link-silk']")
            owners = [x.attrib['href'].split('/')[-1].replace('-', ' ').title() for x in owners]

            age = clean(doc.xpath("//td[@data-test-selector='horse-age']/text()"))
            age = [a.replace('-', '.') for a in age]

            _or = clean(doc.xpath("//td[@data-ending='OR']/text()"))

            ts = clean(doc.xpath("//td[@data-ending='TS']/text()"))

            rpr = clean(doc.xpath("//td[@data-ending='RPR']/text()"))

            st = doc.xpath("//span[@data-ending='st']/text()")
            lb = doc.xpath("//span[@data-ending='lb']/text()")
            wgt = [a.strip() + '-' + b.strip() for a, b in zip(st, lb)]
            lbs = [int(a.strip()) * 14 + int(b.strip()) for a, b in zip(st, lb)]

            headgear = doc.xpath("//td[contains(@class, 'rp-horseTable__wgt')]")
            gear = []
            for h in headgear:
                span = h.find('span[@class="rp-horseTable__headGear"]')
                if span is not None:
                    try:
                        gear.append(span.text + span[1].text.strip())
                    except:
                        gear.append(span.text)
                else:
                    gear.append('')

            info = doc.xpath('//div[@class="rp-raceInfo"]')[0].find('.//li').findall('.//span[@class="rp-raceInfo__value"]')

            times = []

            if len(info) == 3:
                winning_time = clean(info[1].text.split("("))[0].split()

                if winning_time[0] == '0.0.00s' or winning_time[0] == '0.00s':
                    try:
                        winning_time = info[1].text.split("(")[1].lower().replace('fast by', '').strip().strip(')').split()
                    except IndexError:
                        times = ['-' for x in range(len(pos))]

            elif len(info) == 2:
                winning_time = info[0].text.split("(")[0].split()

                if winning_time[0] == '0.0.00s' or winning_time[0] == '0.00s':
                    try:
                        winning_time = info[0].text.split("(")[1].lower().replace('fast by', '').strip().strip(')').split()
                    except IndexError:
                        times = ['-' for x in range(len(pos))]
            else:
                print(f'ERROR: (winning time) {date} {course_name} {r_time}.')

            if '-' not in times:
                if len(winning_time) > 1:
                    try:
                        win_time = float(winning_time[0].replace("m", '')) * 60 + float(winning_time[1].strip("s"))
                    except ValueError:
                        print(f'ERROR: (winning time) {date} {course} {r_time}.')
                else:
                    try:
                        win_time = float(winning_time[0].strip("s"))
                    except ValueError:
                        print(f'ERROR: (winning time) {date} {course} {r_time} {winning_time[0]}.')

                try:
                    times = calculate_times(win_time, time_btn, going, code, course)
                except:
                    print("times error:", race)
                    sys.exit()
            
            dec = fraction_to_decimal([sp.strip('F').strip('J').strip('C').strip() for sp in sps])

            race_name = race_name.replace("'", "")

            for num, p, pr, dr, bt, ovr_bt, n, nat, sp, dc, time, j, tr, a, s, o, rp, t, w, l, g, com, sire, dam, damsire, owner in \
            zip(numbers, pos, prize, draw, btn, ovr_btn, name, nats, sps, dec, times, jock, trainer, age, sex, _or, rpr, ts,\
                wgt, lbs, gear, coms, sires, dams, damsires, owners):

                sire = sire.replace("'", '')
                dam = dam.replace("'", '')
                damsire = damsire.replace("'", '')
                j = j.replace("'", '')
                tr = tr.replace("'", '')
                com = com.replace('\n', '').strip()
                
                csv.write((
                    f'{date},{course},{r_time},{race_name},{race_type},{race_class},{pattern},'
                    f'{rating_band},{age_band},{sex_rest},{distance},{dist_y},{dist_m},{dist_f},'
                    f'{going},{num},{p},{ran},{dr},{bt},{ovr_bt},{n} {nat},{sp},{dc},{a},{s},{w},'
                    f'{l},{g},{time},{j},{tr},{o},{rp},{t},{pr},{sire},{dam},{damsire},{owner},{com}\n'
                ))
        print(f'\nFinished scraping. {years}.csv saved in RPScraper/data/{code}/{target.lower()}')


def parse_args(args=sys.argv):
    if len(args) == 1:
        if "help" in args or "options" in args or "?" in args:
            options(args[0])
        elif "clear" in args:
            os.system("cls" if os.name == "nt" else "clear")
        elif "quit" in args or "q" in args or "exit" in args:
            sys.exit()
        elif "regions" in args:
            print_regions()
        elif "courses" in args:
            print_courses()
    elif len(args) == 2:
        if args[0] == "regions":
            region_search(args[1])
        elif args[0] == "courses":
            if valid_region(args[1]):
                print_courses(args[1])
            else:
                course_search(args[1])
    elif len(args) == 3:
        if args[0] == '-d' or args[0] == 'date':
            if not valid_region(args[2]):
                return print("Invalid region.")

            if check_date(args[1]):
                if '-' in args[1]:
                    start_year, start_month, start_day = args[1].split('-')[0].split('/')
                    end_year, end_month, end_day = args[1].split('-')[1].split('/')

                    start_date = date(int(start_year), int(start_month), int(start_day))
                    end_date = date(int(end_year), int(end_month), int(end_day))

                    dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
                else:
                    year, month, day = args[1].split('/')

                    dates = [date(int(year), int(month), int(day))]

                races = []

                for d in dates:
                    for link in get_race_links(d, args[2]):
                        races.append(link)

                scrape_races(races, args[2], args[1].replace('/', '_'), '')
                
            else:
                return print('Invalid date. Expected format: YYYY/MM/DD')
        else:
            if valid_region(args[0]):
                region = args[0]
            elif valid_course(args[0]):
                course = args[0]
            else:
                return print("Invalid course or region.")

            if "jumps" in args or "jump" in args or "-j" in args:
                code = "jumps"
            elif "flat" in args or "-f" in args:
                code = "flat"
            else:
                return print("Invalid racing code. -f, flat or -j, jumps.")

            if "-" in args[1]:
                try:
                    years = [str(x) for x in range(int(args[1].split("-")[0]), int(args[1].split("-")[1]) + 1)]
                except ValueError:
                    return print(f"\nINVALID YEAR: must be in range 1996-{datetime.today().year}.\n")
            else:
                years = [args[1]]
            if not valid_years(years):
                return print(f"\nINVALID YEAR: must be in range 1988-{datetime.today().date} for flat and "
                             f"1987-{datetime.today().year-1} for jumps.\n")

            if code == "jumps":
                if int(years[-1]) > datetime.today().year-1:
                    return print(f"\nINVALID YEAR: the latest jump season started in {datetime.today().year-1}.\n")

            if "region" in locals():
                tracks = [course[0] for course in courses(region)]
                names = [course_name(track) for track in tracks]
                scrape_target = region
                print(f"Scraping {code} results from {scrape_target} in {args[1]}...")
            else:
                tracks = [course]
                names = [course_name(course)]
                scrape_target = course
                print(f"Scraping {code} results from {course_name(scrape_target)} in {args[1]}...")

            races = get_races(tracks, names, years, code, x_y())
            scrape_races(races, course_name(scrape_target), args[1], code)
    else:
        options()


def main():
    if len(sys.argv) > 1:
        sys.exit(options())
    try:
        import readline
        completions = Completer(["courses", "regions", "options", "help", "quit", "exit", "clear", "flat", "jumps", "date"])
        readline.set_completer(completions.complete)
        readline.parse_and_bind('tab: complete')
    except ModuleNotFoundError:  # windows
        pass

    while True:
        args = input('[RPScraper]> ').lower().strip()
        parse_args([arg.strip() for arg in args.split()])


if __name__ == '__main__':
    main()
