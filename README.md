

Please copy he folder.

1. Create python environment using
for ubunto only --> sudo apt install python3-venv
for windows --> Anyone from the below, preferebaly b).
                a). python -m venv venv
                b). py -m venv venv
                c). python3 -m venv venv

2. Activate envioronment as below:
                for Linux -- > source aurva/bin/activate
                for windows -->  .\venv\Scripts\activate

3. Install Libraries by
                pip install -r requirements.txt

4. Download & Install PostgreSQL Database from the link
                https://www.postgresql.org/download/

5. Run pgAdmin (from PostgreSQL)
            a. Create a database ex. "delivery_data"
            b. Create a user or can keep default as "postgres"
            c. Create password as ex. "my_password"
6. Update the credentials (as per 5th steps) in app/creden.txt file
            a. Database Name
            b. User Name
            c. Password
7. Run db_schema file to create Database
# Please change the database name, username, and password to your own

Now the setup has been done to use

Run it as development server by following :
         flask run --debug

         



