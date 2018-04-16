# node-query-bot

## WIP
- Create a file called settings.json and add the following details
```
{
     "slack_token":` "YOUR_SLACK_TOKEN_HERE",
     "dbstring": "postgres://USERNAME:PASSWORD@DBURL/DBNAME",
     "host": "IDF_FTP_HOST",
     "user": "IDF_FTP_USERNAME",
     "password": "IDF_FTP_PASSWORD"
   }
```
- Add your queries to queries/.
- Modify code as needed.

## TODO
- Make this modular and adaptable. (eg. remove IDF)
- Make it configurable, you shouldn't need to edit the code to be able to add new queries on a new schedule.