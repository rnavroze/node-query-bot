const SlackBot = require('slackbots');
const SlackUpload = require('node-slack-upload');
const pg = require('pg');
const schedule = require('node-schedule');
const fs = require('fs');
const unidecode = require('unidecode');
const SFTPClient = require('ssh2').Client;

// constants
const settings = JSON.parse(fs.readFileSync('settings.json', 'utf-8'));
const idfFTP = { host: settings.host, user: settings.user, password: settings.password };
const connectionString = process.env.DATABASE_URL || settings.dbstring;
const realmax = 60030;
const apmax_sem = 27500;

// hardcode the queries (for now)
const queries = {
	real:                   fs.readFileSync('queries/real.sql', 'utf-8'),
	realAp:                 fs.readFileSync('queries/realAp.sql', 'utf-8'),
	daal:                   fs.readFileSync('queries/daal.sql', 'utf-8'),
	ldaPerLC:               fs.readFileSync('queries/ldaPerLC.sql', 'utf-8'),
	idf:                    fs.readFileSync('queries/idf.sql', 'utf-8'),
	peopleShiftedFormat:    fs.readFileSync('queries/peopleShiftedFormat.sql', 'utf-8'),
	peopleSignUp:           fs.readFileSync('queries/peopleSignUp.sql', 'utf-8'),
	peopleLanguages:        fs.readFileSync('queries/peopleLanguages.sql', 'utf-8'),
	peopleBackgrounds:      fs.readFileSync('queries/peopleBackgrounds.sql', 'utf-8'),
	copSignUp:              fs.readFileSync('queries/copSignUp.sql', 'utf-8'),
	nps:                    fs.readFileSync('queries/nps.sql', 'utf-8'),
	processTime:            fs.readFileSync('queries/processTime.sql', 'utf-8'),
	ixpData:                fs.readFileSync('queries/ixpData.sql', 'utf-8'),
	standards:              fs.readFileSync('queries/standards.sql', 'utf-8'),
	expaMembership:         fs.readFileSync('queries/expaMembership.sql', 'utf-8'),
	analytics:              fs.readFileSync('queries/analytics.sql', 'utf-8'),
	opportunities:          fs.readFileSync('queries/opportunities.sql', 'utf-8'),
	opportunityBackgrounds: fs.readFileSync('queries/opportunityBackgrounds.sql', 'utf-8'),
	organizations:          fs.readFileSync('queries/organizations.sql', 'utf-8'),
	tomas:                  fs.readFileSync('queries/tomas.sql', 'utf-8'),
	tcs:                    fs.readFileSync('queries/dappy_tcs.sql', 'utf-8')
};

// create a bot
const slack_token = settings.slack_token;
const bot = new SlackBot({
	token: slack_token, // Add a bot https://my.slack.com/services/new/bot and put the token
	name:  'Data Bot'
});

const uploader = new SlackUpload(slack_token);

let params = {
	icon_emoji: ':lion_face:',
	link_names: true
};

// set up the scheduler
// [s] m h d m dw
let mondayDAAL = schedule.scheduleJob('0 0 11 * * 1', sendDAAL);
let raed1 = schedule.scheduleJob('0 0 12 * * 4', sendDAAL);
let tuesdayProcessTime = schedule.scheduleJob('0 0 11 * * 2', sendProcessTime);
let raed2 = schedule.scheduleJob('0 0 12 * * 4', sendProcessTime);
let wednesdayNPS = schedule.scheduleJob('0 0 11 * * 3', sendNPS);
let thursdayPeople = schedule.scheduleJob('0 0 11 * * 4', sendPeopleData);
let fridayOpportunities = schedule.scheduleJob('0 0 11 * * 5', sendOpportunitiesData);
let saturdayOrganizations = schedule.scheduleJob('0 0 11 * * 6', sendOrganizationsData);
let sundayTM = schedule.scheduleJob('0 0 11 * * 7', sendTMData);

let morningGetReal = schedule.scheduleJob('0 0 10 * * *', getReal);
let dailyIDF = schedule.scheduleJob('0 30 0 * * 3', uploadIDF);
let dailyPartners = schedule.scheduleJob('0 0 9 * * *', partnerSignups);
let monthlyTCS = schedule.scheduleJob('0 0 13 1 * *', tcsApps);
let weeklyTomas = schedule.scheduleJob('0 0 8 * * 1', tomasData);

let lastThursdayOfEveryMonthMembershipList = schedule.scheduleJob('0 0 12 24 * *', sendMembershipList);

let workingHoursStartAP = schedule.scheduleJob('0 0 4 * * 1-5', () => sendHoursUpdate("Working hours have started in AP."));
let workingHoursStartMEA = schedule.scheduleJob('0 0 10 * * 1-5', () => sendHoursUpdate("Working hours have started in Europe."));
let workingHoursStartAm = schedule.scheduleJob('0 0 15 * * 1-5', () => sendHoursUpdate("Working hours have started in Americas."));

let workingHoursEndEurope = schedule.scheduleJob('0 0 19 * * 1-5', () => sendHoursUpdate("Working hours have ended in Europe."));
let workingHoursEndAP = schedule.scheduleJob('0 0 15 * * 1-5', () => sendHoursUpdate("Working hours have ended in AP."));
let workingHoursEndMEA = schedule.scheduleJob('0 0 18 * * 1-5', () => sendHoursUpdate("Working hours have ended in MEA."));
let workingHoursEndAm = schedule.scheduleJob('0 0 1 * * 2-6', () => sendHoursUpdate("Working hours have ended in Americas."));

//let dailyDAAL = schedule.scheduleJob('0 30 12 * * *', sendDAAL);

// functions
async function uploadIDF()
{
	// FIXME: lots of hardcoding in this function
	console.log("IDF upload started");
	let c = new SFTPClient();
	c.on('ready', function () {
		c.sftp(async function (err, sftp) {
			if (err)
			{
				console.log(err);
				return false;
			}
			console.log("SFTP ready");
			
			// generate IDF
			let query = await client.query(queries.idf);
			console.log("Got IDF query");
			
			require("csv-to-array")({
				file:    "LC_Substitutions.csv",
				columns: ["from", "to"]
			}, async function (err, array) {
				if (err)
					console.log(err);
				
				let subsArray = array;
				
				// create offices file first
				let queryOffices = await client.query("SELECT offices.name \"name\", p.name \"parent\" FROM offices JOIN offices p ON offices.parent_id = p.id WHERE offices.deleted_at IS NULL AND p.deleted_at IS NULL"); // FIXME: hardcoded query
				let queryOfficesArray = [];
				let queryOfficesProcessed = "OU ID,OU Name,Parent ID,Owner ID,Description,Active,Allow Reconciliation,Facility Type,Country,Time Zone,Address #1,Address #2,City,State/Province,Postal Code,Contact,Phone,Fax,Email,Occupancy,Approval Required,On Site\n";
				let officeParents = [];
				
				for (let x in queryOffices.rows)
				{
					if (queryOffices.rows.hasOwnProperty(x))
					{
						let thisOfficeName;
						// See if there's a matching substitution
						let eID = queryOffices.rows[x]["name"];
						let match = subsArray.find(o => o.from === eID);
						if (match !== undefined)
							thisOfficeName = match.to;
						// No? Then just strip all non-alphanumeric characters
						else
							thisOfficeName = queryOffices.rows[x]["name"].replace(/[^A-Za-z0-9 ]/g, '');
						
						queryOfficesArray.push(thisOfficeName);
						officeParents[thisOfficeName] = queryOffices.rows[x]["parent"];
					}
				}
				
				let queryOfficesArrayUnique = queryOfficesArray.filter(function (item, pos, self) {
					return self.indexOf(item) === pos;
				});
				
				for (let x in queryOfficesArrayUnique)
					queryOfficesProcessed += "\"" + queryOfficesArrayUnique[x] + "\",\"" + queryOfficesArrayUnique[x] + "\",\"" + officeParents[queryOfficesArrayUnique[x]] + "\",,,,,,NL,1,,,,,,,,,,,\n";
				
				fs.writeFileSync('Entities.csv', queryOfficesProcessed, 'binary');
				console.log("Wrote entities to file");
				
				// upload file
				let readStreamEY = fs.createReadStream("Entities.csv");
				let writeStreamEY = sftp.createWriteStream("/Datafeed/Location.txt");
				
				readStreamEY.pipe(writeStreamEY);
				
				let queryRowsProcessed = [];
				for (let k in query.rows)
				{
					if (query.rows.hasOwnProperty(k))
					{
						queryRowsProcessed[k] = query.rows[k];
						let eID = queryRowsProcessed[k]["Entity ID"];
						let match = subsArray.find(o => o.from === eID);
						if (match !== undefined)
							queryRowsProcessed[k]["Entity ID"] = match.to;
						else
							queryRowsProcessed[k]["Entity ID"] = queryRowsProcessed[k]["Entity ID"].replace(/[^A-Za-z0-9 ]/g, '');
					}
				}
				console.log("Ran substitutions");
				
				let csv = json2csv(queryRowsProcessed);
				console.log("Wrote csv to variable");
				
				// convert variable to unicode
				csv = unidecode(csv);
				console.log("Converted csv to ascii");
				
				fs.writeFileSync('IDF.csv', csv, 'binary');
				console.log("Wrote csv to file");
				
				// upload file
				let readStream = fs.createReadStream("IDF.csv");
				let writeStream = sftp.createWriteStream("/Datafeed/User.txt");
				
				writeStream.on('close', function () {
					console.log("Upload complete");
					//bot.postMessageToUser('raihan', "IDF file has been uploaded to CornerStone.", params);
					bot.postMessageToUser('hans', "IDF file has been uploaded to CornerStone.", params);
					c.end();
				});
				
				writeStream.on('end', function () {
					console.log("SFTP closed");
					c.close();
				});
				
				// initiate transfer of file
				readStream.pipe(writeStream);
			})
		})
	}).connect({
		//debug:      console.log,
		algorithms: {
			serverHostKey: ['ssh-dss'],
			cipher:        ['blowfish-cbc']
		},
		host:       idfFTP.host,
		user:       idfFTP.user,
		password:   idfFTP.password
	});
}

// idf test
async function idfTest()
{
	//console.log("IDF Test is on!");
	//let queryString = queries.idf;
	//
	//// generate IDF
	//let query = await client.query(queryString);
	//console.log("Got IDF query");
	//
	//// get LC substitutions
	//let subsArray;
	//
	//require("csv-to-array")({
	//	file:    "LC_Substitutions.csv",
	//	columns: ["from", "to"]
	//}, async function (err, array) {
	//	if (err)
	//		console.log(err);
	//
	//	subsArray = array;
	//
	//	let queryRowsProcessed = [];
	//	for (let k in query.rows)
	//	{
	//		if (query.rows.hasOwnProperty(k))
	//		{
	//			queryRowsProcessed[k] = query.rows[k];
	//			let eID = queryRowsProcessed[k]["Entity ID"];
	//			let match = subsArray.find(o => o.from === eID);
	//			if (match !== undefined)
	//				queryRowsProcessed[k]["Entity ID"] = match.to;
	//		}
	//	}
	//	console.log("Ran substitutions");
	//
	//	let csv = json2csv(queryRowsProcessed);
	//	console.log("Wrote csv to variable");
	//
	//
	//	// convert variable to unicode
	//	csv = unidecode(csv);
	//	console.log("Converted csv to ascii");
	//
	//	fs.writeFileSync('CornerTest.csv', csv, 'binary');
	//	console.log("Wrote csv to file");
	//
	//	uploader.uploadFile({
	//		file:           fs.createReadStream('CornerTest.csv'),
	//		filetype:       'csv',
	//		title:          'CornerTest',
	//		initialComment: "Here",
	//		channels:       'larhans'
	//	}, function (err, data) {
	//		if (err)
	//			console.error(err);
	//		else
	//			console.log('Uploaded file details: ', data);
	//	});
	//
	//});
}

// lar-issa function
async function partnerSignups()
{
	console.log("Partner signups going to LarIssa");
	let queryString = "SELECT * FROM partners_signups";
	
	// generate IDF
	let query = await client.query(queryString);
	console.log("Got Partner Signups query");
	
	let csv = json2csv(query.rows);
	console.log("Wrote csv to variable");
	
	// convert variable to unicode
	csv = unidecode(csv);
	console.log("Converted csv to ascii");
	
	fs.writeFileSync('PartnerSignups.csv', csv, 'binary');
	console.log("Wrote csv to file");
	
	uploader.uploadFile({
		file:           fs.createReadStream('PartnerSignups.csv'),
		filetype:       'csv',
		title:          'Partner Signups',
		initialComment: "issa sucks",
		channels:       'laromar'
	}, function (err, data) {
		if (err)
			console.error(err);
		else
			console.log('Uploaded file details: ', data);
	});
}

// dappy function
async function tcsApps()
{
	console.log("TCS apps going to DappyBird");
	let queryString = queries.tcs;
	
	// generate IDF
	let query = await client.query(queryString);
	console.log("Got TCS query");
	
	let csv = json2csv(query.rows);
	console.log("Wrote csv to variable");
	
	// convert variable to unicode
	csv = unidecode(csv);
	console.log("Converted csv to ascii");
	
	fs.writeFileSync('TCS.csv', csv, 'binary');
	console.log("Wrote csv to file");
	
	uploader.uploadFile({
		file:           fs.createReadStream('TCS.csv'),
		filetype:       'csv',
		title:          'TCS',
		initialComment: "Here",
		channels:       'dappybird'
	}, function (err, data) {
		if (err)
			console.error(err);
		else
			console.log('Uploaded file details: ', data);
	});
}

// dappy function
async function tomasData()
{
	console.log("Data going to TomasBro");
	let queryString = queries.tomas;
	
	// generate IDF
	let query = await client.query(queryString);
	console.log("Got Tomas query");
	
	let csv = json2csv(query.rows);
	console.log("Wrote csv to variable");
	
	// convert variable to unicode
	csv = unidecode(csv);
	console.log("Converted csv to ascii");
	
	fs.writeFileSync('Tomas.csv', csv, 'binary');
	console.log("Wrote csv to file");
	
	uploader.uploadFile({
		file:           fs.createReadStream('Tomas.csv'),
		filetype:       'csv',
		title:          'Tomas',
		initialComment: "Here",
		channels:       'tomasbro'
	}, function (err, data) {
		if (err)
			console.error(err);
		else
			console.log('Uploaded file details: ', data);
	});
}

async function getReal(callback)
{
	console.log("Getting real");
	let query = await client.query(queries.real);
	
	let adjusted = query.rows[0].c;
	let realcount = realmax - adjusted;
	bot.postMessageToChannel('general', `Good morning! At ${adjusted.toLocaleString()}, we are ${realcount.toLocaleString()} exchanges away from being real.`, params);
	
	query = await client.query(queries.realAp);
	
	adjusted = query.rows[0].c;
	realcount = apmax_sem - adjusted;
	//bot.postMessageToChannel('general', `And at ${adjusted.toLocaleString()}, we are ${realcount.toLocaleString()} approvals away from hitting our peak target.`, params);
	
	let dow = new Date().getDay();
	//if (dow > 0 && dow < 6)
	//bot.postMessageToChannel('general', 'By the way, working hours have started in Europe & MEA.');
	
	
	if (callback !== undefined)
		callback();
}

async function sendAnalytics()
{
	return sendTypicalQuery(queries.analytics, 'Analytics', 'Hey there! This is the analytics file - think of it like Core on a global level. Timespan: 1st August 2017 to right now.');
}

async function sendDAAL()
{
	sendTypicalQuery(queries.copSignUp, 'COP Sign Ups', "Here is the list of companies who signed up for COP (!!! not POP !!!).");
	sendAnalytics();
	return sendTypicalQuery(queries.daal, 'DAAL', 'Happy day! Here is the DAAL file for all EPs approved and realized since 1/1/17 to today. Some columns that identify personal data are removed to comply with GDPR.');
}

async function sendProcessTime()
{
	return sendTypicalQuery(queries.processTime, 'Process_Time', "Hope you're having a great day! This is the Process Time analytics file for all EPs who signed up since 1st July 2017 to today.");
}

async function sendTMData()
{
	sendTypicalQuery(queries.ldaPerLC, 'Membership_LDA_per_LC', "It's data time! This one is membership per LC (as per EXPA data) and # people filled LDA per LC, since 1st August 2017.");
	sendTypicalQuery(queries.ixpData, 'iXP', "Here's the iXP data since 1st September.");
	sendTypicalQuery(queries.peopleShiftedFormat, 'New_Format', "Here's the number of members who changed their membership format on EXPA.");
}

async function sendPeopleData()
{
	sendTypicalQuery(queries.peopleSignUp, 'People_Sign_Up', "Today is People day! Here is sign up data since 1st January 2017.");
	sendTypicalQuery(queries.peopleLanguages, 'People_Languages', "Here's people and their langauges.");
	sendTypicalQuery(queries.peopleBackgrounds, 'People_Backgrounds', "And here's people and their backgrounds! As always, no personal data because #GDPR. Have a good day!");
}

async function sendNPS()
{
	sendTypicalQuery(queries.nps, 'NPS', "It's Value Delivery day! Here is NPS data since 1st September.");
	sendTypicalQuery(queries.standards, 'Standards', "Here is Standards from 1st September.")
}

async function sendMembershipList()
{
	return sendTypicalQuery(queries.expaMembership, 'EXPA_Membership_Emails', "It's that day of the month again. Here's the list of all members on EXPA!");
}

async function sendOpportunitiesData()
{
	sendTypicalQuery(queries.opportunities, 'Opportunities', "What's up? Here is a list of all opportunities and their data, created after 1st January 2017.");
	return sendTypicalQuery(queries.opportunityBackgrounds, 'Opportunities', "Here's the opportunity backgrounds.");
}


async function sendOrganizationsData()
{
	return sendTypicalQuery(queries.organizations, 'Organizations', "Hey! Here is all time data for all organizations on the system.");
}


async function sendTypicalQuery(querystring, filename, message, callback)
{
	let d = new Date();
	console.log(filename + " file generation started on " + d);
	let query = await client.query(querystring);
	console.log("Got " + filename + " query");
	sendFile(query, filename, message);
	
	if (callback !== undefined)
		callback();
}

function sendMessageToGeneral(message)
{
	bot.postMessageToChannel('general', message, params);
}

function sendHoursUpdate(message)
{
	bot.postMessageToChannel('calendarupdates', message, params);
}

function json2csv(rows)
{
	if (rows[0] === null)
		return false;
	
	let keysRow = rows[0];
	let csvString = "sep=;\n";
	
	csvString += Object.keys(keysRow).join(";");
	csvString += "\n";
	
	for (let row in rows)
	{
		let cols = Object.values(rows[row]);
		
		for (let col in cols)
		{
			if (cols[col] === null || cols[col] === '')
				continue;
			
			cols[col] = '"' + cols[col].toString().replace(/"/g, "'") + '"';
		}
		
		csvString += cols.join(";");
		csvString += "\n";
	}
	return csvString;
}

async function sendFile(query, filename, message)
{
	let d = new Date();
	let datestring = (d.getDate()) + '_' + (d.getMonth() + 1) + '_' + (d.getFullYear());
	
	let csv = json2csv(query.rows);
	console.log("Wrote csv to variable");
	
	csv = unidecode(csv);
	console.log("Converted csv to ascii");
	
	fs.writeFileSync(filename + '_' + datestring + '.csv', csv, 'binary');
	console.log("Wrote csv to system");
	
	uploader.uploadFile({
		file:           fs.createReadStream(filename + '_' + datestring + '.csv'),
		filetype:       'csv',
		title:          filename + '_' + datestring,
		initialComment: message,
		channels:       'datarequests'
	}, function (err, data) {
		if (err)
			console.error(err);
		else
			console.log('Uploaded file details: ', data);
	});
}

// connect PGSQL
const client = new pg.Client(connectionString);
client.connect();

bot.on('start', function () {
	console.log('Databot online on ' + (new Date()) + '!');

	//getReal();
	//sendTypicalQuery(queries.standards, 'Standards', "Here is Standards from 1st September.")
	//sendTypicalQuery(queries.peopleSignUp, 'People_Sign_Up', "Today is People day! Here is sign up data since 1st January 2017.");
	//tcsApps();
	//idfTest();
	//sendProcessTime();
	//sendDAAL();
	//uploadIDF();
	//getRealTest();
	//sendNPS();
	//sendMessageToGeneral("bye :(");
	//sendHoursUpdate("hi guys");
	//bot.postMessageToUser('raihan', `Test`, params);
	//partnerSignups();
	//sendTMData();
	//uploadIDF();
	//sendOpportunitiesData();
	//tomasData();
	//sendPeopleData();
});

bot.on('message', async function (event) {
	//
});