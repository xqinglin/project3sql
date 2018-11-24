import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
conn = sqlite3.connect(DBNAME)
cur = conn.cursor()
statement = '''
     DROP TABLE IF EXISTS 'Bars';
 '''

## Create the table
statement1 = '''
    CREATE TABLE Bars (
       Id integer PRIMARY KEY,
       Company TEXT NOT NULL,
       SpecificBeanBarName TEXT NOT NULL,
       REF TEXT NOT NULL,
       ReviewDate TEXT NOT NULL,
       CocoaPercent REAL NOT NULL,
       CompanyLocationId Integer,
       Rating REAL NOT NULL,
       BeanType TEXT,
       BroadBeanOriginId Integer,
       FOREIGN KEY (CompanyLocationId) REFERENCES Countries(Id),
       FOREIGN KEY (BroadBeanOriginId) REFERENCES Countries(Id)
    ) ;
'''
statement2 = '''
     DROP TABLE IF EXISTS 'Countries';
 '''
statement3 = '''
    CREATE TABLE Countries (
       Id integer PRIMARY KEY,
       Alpha2 TEXT ,
       Alpha3 TEXT ,
       EnglishName TEXT ,
       Region TEXT ,
       Subregion TEXT ,
       Population Integer,
       Area REAL
    ) ;
'''
cur.execute(statement)
cur.execute(statement1)
cur.execute(statement2)
cur.execute(statement3)
conn.commit()

## do the json file
file_json = open('countries.json')
file_load = json.load(file_json)

for row in file_load:
    statement5 = '''
                    INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region,Subregion,Population,Area)
                    VALUES (?,?,?,?,?,?,?)
                '''
    cur.execute(statement5, (row['alpha2Code'], row['alpha3Code'], row['name'], row['region'], row['subregion'], row['population'], row['area'],))
cur.execute(statement5, ('', '', 'Unkown', '', '', 0, 0.0,))
## open cvs file
file_cvs = 'flavors_of_cacao_cleaned.csv'
print(file_cvs)
with open(file_cvs, newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    count = 0
    for row in spamreader:
        # print(', '.join(row))
        if count != 0:
            id_BroadBeanOriginId= cur.execute('SELECT Id FROM  Countries where EnglishName = ?',(row[8],))
            id_BroadBeanOriginId = id_BroadBeanOriginId.fetchall()
            if len(id_BroadBeanOriginId) >= 1:
                id_BroadBeanOriginId = id_BroadBeanOriginId[0][0]
            else:
                id_BroadBeanOriginId= 'NULL'
            ##
            id_CompanyLocationId = cur.execute('SELECT Id FROM  Countries where EnglishName = ?', (row[5],))
            id_CompanyLocationId = id_CompanyLocationId.fetchall()
            if len(id_CompanyLocationId) >= 1:
                id_CompanyLocationId = id_CompanyLocationId[0][0]
            else:
                id_CompanyLocationId = 'NULL'
            coca = float(row[4].replace('%',''))*0.01
            statement4 = '''
                INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate,CocoaPercent,CompanyLocationId,Rating,BeanType, BroadBeanOriginId)
                VALUES (?,?,?,?,?,?,?,?,?)
            '''
            cur.execute(statement4,(row[0],row[1], row[2],row[3], coca, id_CompanyLocationId, row[6], row[7], id_BroadBeanOriginId))
            conn.commit()
            #Company	SpecificBeanBarName	REF	ReviewDate	CocoaPercent	CompanyLocation	Rating	BeanType	BroadBeanOrigin
        count += 1


conn.commit()
conn.close()
print('Part 1 is done' )

# Part 2: Implement logic to process user commands
## sellcountry=<alpha2> | sourcecountry=<alpha2> | sellregion=<name> | sourceregion=<name>, ratings | cocoa, top=<limit> | bottom=<limit>
def get_bar(command):
    statement6 = '''
                SELECT b.SpecificBeanBarName, b.Company,c1.EnglishName, b.Rating,b.CocoaPercent, c2.EnglishName  FROM  Bars as b
                JOIN Countries as c1 on c1.Id=b.CompanyLocationId
                JOIN Countries as c2 on c2.Id = b.BroadBeanOriginId
    '''
    commands = command.split(' ')
    list_commands = {
        'sellcountry': 'c1.Alpha2=',
        'sourcecountry': 'c2.Alpha2=',
        'sellregion': 'c1.Region=',
        'sourceregion':'c2.Region=',
        'top':'Limit ',
        'bottom':'Limit '
    }
    final_command = []
    order = ''
    bot = False
    for c in commands:
        if c.find('=') != -1:
            c = c.split('=')
            if c[0] == 'top':
                order = list_commands[c[0]]+ c[1]
            elif c[0] == 'bottom':
                order = list_commands[c[0]] + c[1]
                bot = True
            else:
                final_command.append(list_commands[c[0]]+ '"'+c[1]+'"')
    if len(final_command) >0:
        final_command = "Where "+", ".join(final_command)
    else:
        final_command=''
    statement6 += final_command
    if bot:
        if 'cocoa' in command:
            statement6 += ' Order by b.cocoaPercent ASC '+order
        else:
            statement6 += ' Order by b.Rating ASC '+order
    else:
        if 'cocoa' in command:
            statement6 += ' Order by b.cocoaPercent DESC '+order
        else:
            statement6 += ' Order by b.Rating DESC '+order
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    res = cur.execute(statement6).fetchall()
    cur.close()
    return res

def get_companies(command):
    commands = command.split(' ')
    list_commands = {
        'country': 'Alpha2=',
        'region': 'Region=',
        'top':'Limit ',
        'bottom':'Limit '
    }
    final_command = []
    order = ''
    bot = False
    for c in commands:
        if c.find('=') != -1:
            c = c.split('=')
            if c[0] == 'top':
                order = list_commands[c[0]]+ c[1]
            elif c[0] == 'bottom':
                order = list_commands[c[0]] + c[1]
                bot = True
            else:
                final_command.append(list_commands[c[0]]+ '"'+c[1]+'"')

    final_command.append('barTypes>4')
    final_command = "Where "+" and ".join(final_command)
    statement6 = '''
                  SELECT Company,EnglishName,ave_cocoa FROM (SELECT b.Company, c1.Region,c1.EnglishName, c1.Alpha2, AVG(b.CocoaPercent) as ave_cocoa, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY b.Company)      
                    '''
    statement7 = '''
                  SELECT Company,EnglishName,barTypes FROM (SELECT b.Company, c1.Region,c1.EnglishName, c1.Alpha2, COUNT( b.SpecificBeanBarName) as barTypes FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY b.Company)      
                    '''
    statement8 = '''
                  SELECT Company,EnglishName,ave_rating FROM (SELECT b.Company, c1.Region,c1.EnglishName, c1.Alpha2, AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY b.Company)      
                    '''
    if bot:
        if 'cocoa' in command:
            statement_final = statement6+ final_command + ' Order by ave_cocoa ASC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes ASC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating ASC '+order
    else:
        if 'cocoa' in command:
            statement_final = statement6 + final_command + ' Order by ave_cocoa DESC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes DESC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating DESC '+order
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    res = cur.execute(statement_final).fetchall()
    conn.close()
    return res


def get_countries(command):
    commands = command.split(' ')
    list_commands = {
        'region': 'Region=',
        'top':'Limit ',
        'bottom':'Limit '
    }
    final_command = []
    order = ''
    bot = False
    for c in commands:
        if c.find('=') != -1:
            c = c.split('=')
            if c[0] == 'top':
                order = list_commands[c[0]]+ c[1]
            elif c[0] == 'bottom':
                order = list_commands[c[0]] + c[1]
                bot = True
            else:
                final_command.append(list_commands[c[0]]+ '"'+c[1]+'"')

    final_command.append('barTypes>4')
    final_command = "Where "+" and ".join(final_command)
    statement6 = '''
                  SELECT EnglishName, Region,ave_cocoa FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.EnglishName)            
                    '''
    statement7 = '''
                  SELECT EnglishName, Region,barTypes FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.EnglishName)           
                    '''
    statement8 = '''
                  SELECT EnglishName, Region,ave_rating  FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.EnglishName)           
                    '''
    if bot:
        if 'cocoa' in command:
            statement_final = statement6+ final_command + ' Order by ave_cocoa ASC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes ASC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating ASC '+order
    else:
        if 'cocoa' in command:
            statement_final = statement6 + final_command + ' Order by ave_cocoa DESC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes DESC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating DESC '+order

    if 'sources' in commands:
        statement_final = statement_final.replace('CompanyLocationId','BroadBeanOriginId')
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    res = cur.execute(statement_final).fetchall()
    conn.close()
    return res

def get_regions(command):
    commands = command.split(' ')
    list_commands = {
        'top':'Limit ',
        'bottom':'Limit '
    }
    final_command = []
    order = ''
    bot = False
    for c in commands:
        if c.find('=') != -1:
            c = c.split('=')
            if c[0] == 'top':
                order = list_commands[c[0]]+ c[1]
            elif c[0] == 'bottom':
                order = list_commands[c[0]] + c[1]
                bot = True
            else:
                final_command.append(list_commands[c[0]]+ '"'+c[1]+'"')

    final_command.append('barTypes>4')
    final_command = "Where "+" and ".join(final_command)
    statement6 = '''
                  SELECT Region,ave_cocoa FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.Region)            
                    '''
    statement7 = '''
                  SELECT Region,barTypes FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.Region)           
                    '''
    statement8 = '''
                  SELECT Region,ave_rating  FROM (SELECT c1.EnglishName, c1.Region, AVG(b.CocoaPercent) as ave_cocoa,AVG(b.Rating) as ave_rating, COUNT( b.SpecificBeanBarName) as barTypes  FROM  Bars as b
                  JOIN Countries as c1 on c1.Id=b.CompanyLocationId  GROUP BY c1.Region)           
                    '''
    if bot:
        if 'cocoa' in command:
            statement_final = statement6+ final_command + ' Order by ave_cocoa ASC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes ASC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating ASC '+order
    else:
        if 'cocoa' in command:
            statement_final = statement6 + final_command + ' Order by ave_cocoa DESC '+order
        elif 'bars_sold'in command:
            statement_final = statement7 + final_command +' Order by barTypes DESC '+order
        else:
            statement_final = statement8 + final_command + ' Order by ave_rating DESC '+order

    if 'sources' in commands:
        statement_final = statement_final.replace('CompanyLocationId','BroadBeanOriginId')
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()
    res = cur.execute(statement_final).fetchall()
    conn.close()
    return res


def toPrint(tuple, mas_length):
    lits_p = []
    for i in range(len(tuple)):
        lits_p.append(str(tuple[i]))
        mas_length[i] = max(len(str(tuple[i])), mas_length[i])

    return mas_length,lits_p
def toPrint_bar(tuple, mas_length):
    lits_p = [tuple[0], tuple[1],tuple[2], str(tuple[3]),str(int(tuple[4]*100))+'%', tuple[5]]
    for i in range(len(tuple)):
        mas_length[i] = max(mas_length[i], len(lits_p[i]))
    return lits_p

    return ' '.join(lits_p)
def process_command(command):
    list_command = command.split(' ')
    if 'bars' == list_command[0]:
        res = get_bar(command)
        mas_length = [0,0,0,0,0,0]
        for_print = []
        for i in res:
            for_print.append(toPrint_bar(i, mas_length))
        for i in for_print:
            cur=''
            for j in range(6):
                width = mas_length[j]
                cur =cur+ '{0: <{width}}'.format(i[j], width=width)+' '
            print(cur)
    elif 'companies' == list_command[0]:
        res = get_companies(command)
        mas_length = [0, 0, 0]
        for_print = []
        for i in res:
            mas_length, lits_p = toPrint(i, mas_length)
            for_print.append(lits_p)
        for i in for_print:
            cur=''
            for j in range(len(i)):
                width = mas_length[j]
                cur =cur+ '{0: <{width}}'.format(i[j], width=width)+' '
            print(cur)
    elif 'countries' == list_command[0]:
        res = get_countries(command)
        mas_length = [0, 0, 0]
        for_print = []
        for i in res:
            mas_length, lits_p = toPrint(i, mas_length)
            for_print.append(lits_p)
        for i in for_print:
            cur = ''
            for j in range(len(i)):
                width = mas_length[j]
                cur = cur + '{0: <{width}}'.format(i[j], width=width) + ' '
            print(cur)
    elif 'regions' == list_command[0]:
        res = get_regions(command)
        mas_length = [0,0]
        for_print = []
        for i in res:
            mas_length, lits_p = toPrint(i, mas_length)
            for_print.append(lits_p)
        for i in for_print:
            cur = ''
            for j in range(len(i)):
                width = mas_length[j]
                cur = cur + '{0: <{width}}'.format(i[j], width=width) + ' '
            print(cur)
    else:
        print('Command not recognized: '+str(command))
        res = ()
    return res


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        ###########
        process_command(response)
        ###########
        if response == 'help':
            print(help_text)
            continue

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
