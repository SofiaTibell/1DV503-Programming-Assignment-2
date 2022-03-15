import mysql.connector
from mysql.connector import errorcode
import pandas as pd

# Setup using documentation from:
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-connecting.html

newDB = mysql.connector.connect(user='root', password='root', host='127.0.0.1', port = '8889')
cursor = newDB.cursor()

DB_NAME = 'my_bakery'

# Try connecting to the database, if unsuccessfully, create the database.
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        print("Database {} created successfully.".format(DB_NAME))
        newDB.database = DB_NAME
    else:
        print(err)
        exit(1)

# Define tables for employees.
TABLES = {}
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "  `id` int(11) NOT NULL,"
    "  `firstname` varchar(14) NOT NULL,"
    "  `lastname` varchar(14) NOT NULL,"
    "  `role` varchar(25) NOT NULL,"
    "  `speciality` varchar(14),"
    "  PRIMARY KEY (`id`), UNIQUE KEY `id` (`id`)"
    ") ENGINE=InnoDB")
# Define tables for products.
TABLES['products'] = (
    "CREATE TABLE `products` ("
    "  `name` varchar(25) NOT NULL,"
    "  `type` varchar(14) NOT NULL,"
    "  `baking_time` int(11) NOT NULL,"
    "  `oven_temperature` int(11),"
    "  `price` int(11) NOT NULL,"
    "  PRIMARY KEY (`name`), UNIQUE KEY `name` (`name`)"
    ") ENGINE=InnoDB")
# Define tables for allergens.
TABLES['allergens'] = (
    "CREATE TABLE `allergens` ("
    "  `name` varchar(25) NOT NULL,"
    "  `gluten` varchar(3) NOT NULL,"
    "  `lactose` varchar(3) NOT NULL,"
    "  `nuts` varchar(3) NOT NULL,"
    "  PRIMARY KEY (`name`), UNIQUE KEY `name` (`name`)"
    ") ENGINE=InnoDB")

# Insert tables in database if they are not there already.
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Remove data from employees, products and allergens.
cursor.execute("TRUNCATE `employees`")
cursor.execute("TRUNCATE `products`")
cursor.execute("TRUNCATE `allergens`")

# Read csv file from relative path.
employeesData = pd.DataFrame(pd.read_csv('employees.csv', delimiter = ','))
productsData = pd.DataFrame(pd.read_csv('products.csv', delimiter = ','))
allergensData = pd.DataFrame(pd.read_csv('allergens.csv', delimiter = ','))


# Replaces value if is nan to string
def checkAndReplaceNan(i):
    if pd.isna(i):
        i = 'NA'
    return i

# Insert employees data in database.
for colum, row in employeesData.iterrows():
            addemployees = ("INSERT INTO employees "
               "(id, firstname, lastname, role, speciality) "
               "VALUES (%s, %s, %s, %s, %s)")
            for i in range(len(row)):
                row[i] = checkAndReplaceNan(row[i]) # Replaces value if is nan to string

            data_employees = (row.id, row.firstname, row.lastname, row.role, row.speciality)
            cursor.execute(addemployees, data_employees)

            # We must save since it is not auto committed by default
            newDB.commit()

# Insert products data in database.
for colum, row in productsData.iterrows():
            addproducts = ("INSERT INTO products "
               "(name, type, baking_time, oven_temperature, price) "
               "VALUES (%s, %s, %s, %s, %s)")
            for i in range(len(row)):
                row[i] = checkAndReplaceNan(row[i])  # Replaces value if is nan to string

            if row.baking_time == 'NA': # Since this needs to be an int we need to change it
               row.baking_time = None
            if row.oven_temperature == 'NA': # Since this needs to be an int we need to change it
               row.oven_temperature = None
            if row.price == 'NA': # Since this needs to be an int we need to change it
                row.price = None

            data_products = (row[0], row.type, row.baking_time, row.oven_temperature, row.price)
            cursor.execute(addproducts, data_products)

            # We must save since it is not auto committed by default
            newDB.commit()

# Insert allergens data in database.
for colum, row in allergensData.iterrows():
            addallergens = ("INSERT INTO allergens "
               "(name, gluten, lactose, nuts) "
               "VALUES (%s, %s, %s, %s)")
            for i in range(len(row)):
                row[i] = checkAndReplaceNan(row[i])  # Replaces value if is nan to string

            data_allergens = (row[0], row.gluten, row.lactose, row.nuts)
            cursor.execute(addallergens, data_allergens)

            # We must save since it is not auto committed by default
            newDB.commit()

# Give user options to choose from in main menu and check input.
def mainMenu():
    choice = input("""
                      1: List all employees and their speciality.
                      2: Search for products and their baking time on the employee's specialty.
                      3: Show number of products sorted on product type.
                      4. Show all glutenfree products and their price.
                      5. Show the avereg oventemperature per products type.
                      6. Search for products and their oven temperature on the employee's specialty.
                      Q. Quit
                      ---------
                      Please choose one option: """)
    if choice == "1":
        listemployeesAndSpeciality() # List all the employees' names and their speciality.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice == "2":
        employee = input("""
                      What is the employee's first name? """)
        employeeProductsSpecialtiesTime(employee) # List products and baking time on employee's specialty.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice == "3":
        listProductsOnType() # List products grouped by type.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice == "4":
        glutenfreeNameAndPrice() # List all glutenfree products and their price.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice == "5":
        averageOvenTemp() # List average oven temperature per product type.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice == "6":
        employee = input("""
                      What is the employee's first name? """)
        employeeProductsSpecialtiesTemp(employee) # List products and oven temperature on employee's specialty.
        returnToMainMenu() # Click any key to return to the main menu.
    elif choice=="Q" or choice=="q":
        cursor.close() # Exit program.
        newDB.close()
    else:
        mainMenu()
# Click any key to return to the main menu.
def returnToMainMenu():
    back = input ("""
                Click any key to return to the main menu.""")
    if input: # 
        mainMenu()
# List all the planet names.
def listemployeesAndSpeciality():
    query = ("SELECT firstname, lastname, speciality FROM employees ")
    cursor.execute(query)
    print("{:<10} {:<10} {}".format("Firstname", "Lastname", "Speciality"))
    for (firstname, lastname, speciality) in cursor:
          print("{:<10} {:<10} {}".format(firstname, lastname, speciality))
    pass
# List products and baking time on employee's specialty.    
def employeeProductsSpecialtiesTime(employeeName):
    queryString = "select products.name, products.baking_time as time from employees join products on products.type = employees.speciality WHERE firstname = '{}';".format(employeeName)
    query = (queryString)
    cursor.execute(query)
    for (name, time) in cursor:
        print("Product: {:<15} Baking time: {} minutes.".format(name, time))
    if cursor.rowcount == 0: # Only print this if the employee dose NOT have a speciality.
        print("This employee dose not have a speciality")
    pass
# List products grouped by type.
def listProductsOnType():
    query = ("SELECT type, count(type) FROM `products` group by type;")
    cursor.execute(query)
    for (type, count) in cursor:
        print("Type: {:<15} Number of: {}".format(
            type, count))
    pass
# List all glutenfree products and their price.
def glutenfreeNameAndPrice():
    try: # Creating a view for Glutenfree products
        notGluten = "no"
        queryString = "create view Glutenfree as select allergens.name, products.price from allergens JOIN products on products.name = allergens.name where allergens.gluten = '{}';".format(notGluten)
        query = (queryString)
        cursor.execute(query)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("Gluten view already exist.")
        else:
            print(err.msg)
    else:
        print("Create Gluten view")
    # Get all glutenfree products and their price
    secondquery = "Select * FROM Glutenfree"
    cursor.execute(secondquery)
    for (name, price) in cursor:
        print("Name: {:<20} Price: {} SEK.".format(
            name, price))
    pass
# List average oven temperature per product type.
def averageOvenTemp():
    query = ("SELECT type, AVG(oven_temperature) FROM `products` group by type;")
    cursor.execute(query)
    for (type, oven_temperature) in cursor:
        if oven_temperature == None: # Print Not avaliable for products with oven temperature of NULL.
            print("type: {:<10} Average oven temperature: Not available".format(
            type))
        else:
            print("type: {:<10} Average oven temperature: {} degrees celsius".format(
                type, oven_temperature))
    pass
# List products and oven temperature on employee's specialty.    
def employeeProductsSpecialtiesTemp(employeeName):
    queryString = "select products.name, products.oven_temperature from employees join products on products.type = employees.speciality WHERE firstname = '{}';".format(employeeName)
    query = (queryString)
    cursor.execute(query)
    for (name, oven_temperature) in cursor:
        if oven_temperature == None: # Print Not avaliable for products with oven temperature of NULL.
            print("Product: {:<15} Oven temperature: Not available".format(name, oven_temperature))
        else:
            print("Product: {:<15} Oven temperature: {} degrees celsius.".format(name, oven_temperature))
    if cursor.rowcount == 0: # Only print this if the employee dose NOT have a speciality.
        print("This employee dose not have a speciality")
    pass
# Run main menu.
mainMenu()