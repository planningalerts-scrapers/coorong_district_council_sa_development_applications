// Parses the development applications at the South Australian Coorong District Council web site
// and places them in a database.
//
// Michael Bone
// 25th February 2019
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const cheerio = require("cheerio");
const request = require("request-promise-native");
const sqlite3 = require("sqlite3");
const urlparser = require("url");
const moment = require("moment");
const pdfjs = require("pdfjs-dist");
const didyoumean2_1 = require("didyoumean2"), didyoumean = didyoumean2_1;
sqlite3.verbose();
const DevelopmentApplicationsUrl = "https://www.coorong.sa.gov.au/page.aspx?u=2084&year={0}";
const CommentUrl = "mailto:council@coorong.sa.gov.au";
// All valid street names, street suffixes, suburb names and hundred names.
let StreetNames = null;
let StreetSuffixes = null;
let SuburbNames = null;
let HundredNames = null;
// Sets up an sqlite database.
async function initializeDatabase() {
    return new Promise((resolve, reject) => {
        let database = new sqlite3.Database("data.sqlite");
        database.serialize(() => {
            database.run("create table if not exists [data] ([council_reference] text primary key, [address] text, [description] text, [info_url] text, [comment_url] text, [date_scraped] text, [date_received] text, [legal_description] text)");
            resolve(database);
        });
    });
}
// Inserts a row in the database if the row does not already exist.
async function insertRow(database, developmentApplication) {
    return new Promise((resolve, reject) => {
        let sqlStatement = database.prepare("insert or replace into [data] values (?, ?, ?, ?, ?, ?, ?, ?)");
        sqlStatement.run([
            developmentApplication.applicationNumber,
            developmentApplication.address,
            developmentApplication.description,
            developmentApplication.informationUrl,
            developmentApplication.commentUrl,
            developmentApplication.scrapeDate,
            developmentApplication.receivedDate,
            developmentApplication.legalDescription
        ], function (error, row) {
            if (error) {
                console.error(error);
                reject(error);
            }
            else {
                console.log(`    Saved application \"${developmentApplication.applicationNumber}\" with address \"${developmentApplication.address}\", description \"${developmentApplication.description}\", legal description \"${developmentApplication.legalDescription}\" and received date \"${developmentApplication.receivedDate}\" to the database.`);
                sqlStatement.finalize(); // releases any locks
                resolve(row);
            }
        });
    });
}
// Constructs a rectangle based on the intersection of the two specified rectangles.
function intersect(rectangle1, rectangle2) {
    let x1 = Math.max(rectangle1.x, rectangle2.x);
    let y1 = Math.max(rectangle1.y, rectangle2.y);
    let x2 = Math.min(rectangle1.x + rectangle1.width, rectangle2.x + rectangle2.width);
    let y2 = Math.min(rectangle1.y + rectangle1.height, rectangle2.y + rectangle2.height);
    if (x2 >= x1 && y2 >= y1)
        return { x: x1, y: y1, width: x2 - x1, height: y2 - y1 };
    else
        return { x: 0, y: 0, width: 0, height: 0 };
}
// Calculates the fraction of an element that lies within a rectangle (as a percentage).  For
// example, if a quarter of the specifed element lies within the specified rectangle then this
// would return 25.
function getPercentageOfElementInRectangle(element, rectangle) {
    let elementArea = getArea(element);
    let intersectionArea = getArea(intersect(rectangle, element));
    return (elementArea === 0) ? 0 : ((intersectionArea * 100) / elementArea);
}
// Calculates the area of a rectangle.
function getArea(rectangle) {
    return rectangle.width * rectangle.height;
}
// Formats (and corrects) an address.
function formatAddress(address) {
    address = address.trim();
    if (address.startsWith("LOT:") || address.startsWith("No Residential Address"))
        return "";
    // Remove the comma in house numbers larger than 1000.  For example, the following addresses:
    //
    //     4,665 Princes HWY MENINGIE 5264
    //     11,287 Princes HWY SALT CREEK 5264
    //
    // would be converted to the following:
    //
    //     4665 Princes HWY MENINGIE 5264
    //     11287 Princes HWY SALT CREEK 5264
    if (/^\d,\d\d\d/.test(address))
        address = address.substring(0, 1) + address.substring(2);
    else if (/^\d\d,\d\d\d/.test(address))
        address = address.substring(0, 2) + address.substring(3);
    let tokens = address.split(" ");
    let postCode = undefined;
    let token = tokens.pop();
    if (/^\d\d\d\d$/.test(token))
        postCode = token;
    else
        tokens.push(token);
    // Ensure that a state code is added before the post code if a state code is not present.
    let state = "SA";
    token = tokens.pop();
    if (["ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA"].includes(token.toUpperCase()))
        state = token.toUpperCase();
    else
        tokens.push(token);
    // Construct a fallback address to be used if the suburb name cannot be determined later.
    let fallbackAddress = (postCode === undefined) ? address : [...tokens, state, postCode].join(" ");
    // Pop tokens from the end of the array until a valid suburb name is encountered (allowing
    // for a few spelling errors).
    let suburbName = undefined;
    for (let index = 1; index <= 4; index++) {
        let suburbNameMatch = didyoumean2_1.default(tokens.slice(-index).join(" "), Object.keys(SuburbNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 1, trimSpaces: true });
        if (suburbNameMatch !== null) {
            suburbName = SuburbNames[suburbNameMatch];
            tokens.splice(-index, index); // remove elements from the end of the array           
            break;
        }
    }
    // Expand any street suffix (for example, this converts "ST" to "STREET").
    token = tokens.pop();
    let streetSuffix = StreetSuffixes[token.toUpperCase()];
    if (streetSuffix === undefined)
        streetSuffix = Object.values(StreetSuffixes).find(streetSuffix => streetSuffix === token.toUpperCase()); // the street suffix is already expanded
    if (streetSuffix === undefined)
        tokens.push(token); // unrecognised street suffix
    else
        tokens.push(streetSuffix); // add back the expanded street suffix
    // Pop tokens from the end of the array until a valid street name is encountered (allowing
    // for a few spelling errors).
    let streetName = undefined;
    for (let index = 1; index <= 5; index++) {
        let streetNameMatch = didyoumean2_1.default(tokens.slice(-index).join(" "), Object.keys(StreetNames), { caseSensitive: false, returnType: didyoumean.ReturnTypeEnums.FIRST_CLOSEST_MATCH, thresholdType: didyoumean.ThresholdTypeEnums.EDIT_DISTANCE, threshold: 1, trimSpaces: true });
        if (streetNameMatch !== null) {
            streetName = streetNameMatch;
            let suburbNames = StreetNames[streetNameMatch];
            tokens.splice(-index, index); // remove elements from the end of the array           
            // If the suburb was not determined earlier then attempt to obtain the suburb based
            // on the street (ie. if there is only one suburb associated with the street).  For
            // example, this would automatically add the suburb to "22 Jefferson CT 5263",
            // producing the address "22 JEFFERSON COURT, WELLINGTON EAST SA 5263".
            if (suburbName === undefined && suburbNames.length === 1)
                suburbName = SuburbNames[suburbNames[0]];
            break;
        }
    }
    // If a post code was included in the original address then use it to override the post code
    // included in the suburb name (because the post code in the original address is more likely
    // to be correct).
    if (postCode !== undefined && suburbName !== undefined)
        suburbName = suburbName.replace(/\s+\d\d\d\d$/, " " + postCode);
    // Reconstruct the address with a comma between the street address and the suburb.
    if (suburbName === undefined || suburbName.trim() === "")
        address = fallbackAddress;
    else {
        if (streetName !== undefined && streetName.trim() !== "")
            tokens.push(streetName);
        let streetAddress = tokens.join(" ").trim();
        address = streetAddress + (streetAddress === "" ? "" : ", ") + suburbName;
    }
    return address;
}
// Parses the details from the elements associated with a single page of the PDF (corresponding
// to a single development application).
function parseApplicationElements(elements, informationUrl) {
    // Get the application number (by finding all elements that are at least 10% within the
    // calculated bounding rectangle).
    let applicationNumberHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "").startsWith("devappno"));
    let applicantHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "applicant");
    let applicationReceivedDateHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "applicationreciveddate:");
    if (applicationReceivedDateHeadingElement === undefined)
        applicationReceivedDateHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "applicationreceiveddate:");
    let propertyDetailsHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "propertydetails:");
    let referralsHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "referrals");
    let totalDevelopmentCostsHeadingElement = elements.find(element => element.text.toLowerCase().replace(/\s/g, "") === "totaldevelopmentcosts:");
    if (applicationNumberHeadingElement === undefined) {
        let elementSummary = elements.map(element => `[${element.text}]`).join("");
        console.log(`Ignoring the page because the "Dev App No." text is missing.  Elements: ${elementSummary}`);
        return undefined;
    }
    // Get the application number.
    let applicationNumberBounds = {
        x: applicationNumberHeadingElement.x + applicationNumberHeadingElement.width,
        y: applicationNumberHeadingElement.y,
        width: applicationNumberHeadingElement.width,
        height: applicationNumberHeadingElement.height
    };
    let applicationNumberElement = elements.find(element => getPercentageOfElementInRectangle(element, applicationNumberBounds) > 10);
    let applicationNumber = (applicationNumberElement === undefined) ? "" : applicationNumberElement.text.replace(/\s/g, "");
    if (applicationNumber === "") {
        let elementSummary = elements.map(element => `[${element.text}]`).join("");
        console.log(`Could not find the application number on the PDF page for the current development application.  The development application will be ignored.  Elements: ${elementSummary}`);
        return undefined;
    }
    console.log(`    Found \"${applicationNumber}\".`);
    // Get the received date.
    let receivedDateBounds = {
        x: applicationReceivedDateHeadingElement.x + applicationReceivedDateHeadingElement.width,
        y: applicationReceivedDateHeadingElement.y,
        width: applicationReceivedDateHeadingElement.width,
        height: applicationReceivedDateHeadingElement.height
    };
    let receivedDateElement = elements.find(element => getPercentageOfElementInRectangle(element, receivedDateBounds) > 10);
    let receivedDate = moment.invalid();
    if (receivedDateElement !== undefined)
        receivedDate = moment(receivedDateElement.text.trim(), "D/MM/YYYY", true); // allows the leading zero of the day to be omitted
    // Get the description.
    if (applicantHeadingElement === undefined)
        console.log(`Could not find the "Applicant" heading on the page and so the development application description may be truncated.`);
    let descriptionBounds = {
        x: applicationNumberElement.x + applicationNumberElement.width,
        y: applicationNumberElement.y,
        width: Number.MAX_VALUE,
        height: (applicantHeadingElement === undefined) ? (applicationNumberElement.height * 2) : (applicantHeadingElement.y - applicationNumberElement.y)
    };
    let description = elements.filter(element => getPercentageOfElementInRectangle(element, descriptionBounds) > 10).map(element => element.text).join(" ").trim().replace(/\s\s+/g, " ");
    // Get the address and legal description.
    let addressBounds = {
        x: propertyDetailsHeadingElement.x,
        y: propertyDetailsHeadingElement.y + propertyDetailsHeadingElement.height,
        width: (referralsHeadingElement === undefined) ? Number.MAX_VALUE : (referralsHeadingElement.x - propertyDetailsHeadingElement.x),
        height: (totalDevelopmentCostsHeadingElement == undefined) ? Number.MAX_VALUE : (totalDevelopmentCostsHeadingElement.y - propertyDetailsHeadingElement.y - 2 * propertyDetailsHeadingElement.height) // some extra padding
    };
    let addressElements = elements.filter(element => getPercentageOfElementInRectangle(element, addressBounds) > 10);
    // Group the address and legal description elements into rows.
    let addressRows = [];
    for (let addressElement of addressElements) {
        let addressRow = addressRows.find(row => Math.abs(row[0].y - addressElement.y) < 5); // approximate Y co-ordinate match
        if (addressRow === undefined)
            addressRows.push([addressElement]); // start a new row
        else
            addressRow.push(addressElement); // add to an existing row
    }
    let address = (addressRows.length < 1) ? "" : addressRows[0].map(element => element.text).join(" ").trim().replace(/\s\s+/g, " ");
    let legalDescription = (addressRows.length < 2) ? "" : addressRows.slice(1).map(row => row.map(element => element.text).join(" ")).join(" ").trim().replace(/\s\s+/g, " ");
    address = formatAddress(address);
    if (address === "") {
        let elementSummary = elements.map(element => `[${element.text}]`).join("");
        console.log(`Could not find an address for the current development application.  The development application will be ignored.  Elements: ${elementSummary}`);
        return undefined;
    }
    return {
        applicationNumber: applicationNumber,
        address: address,
        description: (description === "") ? "No description provided" : description,
        informationUrl: informationUrl,
        commentUrl: CommentUrl,
        scrapeDate: moment().format("YYYY-MM-DD"),
        receivedDate: receivedDate.isValid() ? receivedDate.format("YYYY-MM-DD") : "",
        legalDescription: legalDescription
    };
}
// Parses the development applications in the specified date range.
async function parsePdf(url) {
    console.log(`Reading development applications from ${url}.`);
    let developmentApplications = [];
    // Read the PDF.
    let buffer = await request({ url: url, encoding: null, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);
    // Parse the PDF.  Each page has the details of multiple applications.  Note that the PDF is
    // re-parsed on each iteration of the loop (ie. once for each page).  This then avoids large
    // memory usage by the PDF (just calling page._destroy() on each iteration of the loop appears
    // not to be enough to release all memory used by the PDF parsing).
    for (let pageIndex = 0; pageIndex < 5000; pageIndex++) { // limit to an arbitrarily large number of pages (to avoid any chance of an infinite loop)
        let pdf = await pdfjs.getDocument({ data: buffer, disableFontFace: true, ignoreErrors: true });
        if (pageIndex >= pdf.numPages)
            break;
        console.log(`Reading and parsing applications from page ${pageIndex + 1} of ${pdf.numPages}.`);
        let page = await pdf.getPage(pageIndex + 1);
        let textContent = await page.getTextContent();
        let viewport = await page.getViewport(1.0);
        let elements = textContent.items.map(item => {
            let transform = pdfjs.Util.transform(viewport.transform, item.transform);
            // Work around the issue https://github.com/mozilla/pdf.js/issues/8276 (heights are
            // exaggerated).  The problem seems to be that the height value is too large in some
            // PDFs.  Provide an alternative, more accurate height value by using a calculation
            // based on the transform matrix.
            let workaroundHeight = Math.sqrt(transform[2] * transform[2] + transform[3] * transform[3]);
            return { text: item.str, x: transform[4], y: transform[5], width: item.width, height: workaroundHeight };
        });
        // Release the memory used by the PDF now that it is no longer required (it will be
        // re-parsed on the next iteration of the loop for the next page).
        await pdf.destroy();
        if (global.gc)
            global.gc();
        // Sort the elements by Y co-ordinate and then by X co-ordinate.
        let elementComparer = (a, b) => (a.y > b.y) ? 1 : ((a.y < b.y) ? -1 : ((a.x > b.x) ? 1 : ((a.x < b.x) ? -1 : 0)));
        elements.sort(elementComparer);
        let developmentApplication = parseApplicationElements(elements, url);
        if (developmentApplication !== undefined)
            if (!developmentApplications.some(otherDevelopmentApplication => otherDevelopmentApplication.applicationNumber === developmentApplication.applicationNumber)) // ignore duplicates
                developmentApplications.push(developmentApplication);
    }
    return developmentApplications;
}
// Gets a random integer in the specified range: [minimum, maximum).
function getRandom(minimum, maximum) {
    return Math.floor(Math.random() * (Math.floor(maximum) - Math.ceil(minimum))) + Math.ceil(minimum);
}
// Pauses for the specified number of milliseconds.
function sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}
// Parses the development applications.
async function main() {
    // Ensure that the database exists.
    let database = await initializeDatabase();
    // Read the files containing all possible street names, street suffixes, suburb names and
    // hundred names.  Note that these are not currently used.
    StreetNames = {};
    for (let line of fs.readFileSync("streetnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetNameTokens = line.toUpperCase().split(",");
        let streetName = streetNameTokens[0].trim();
        let suburbName = streetNameTokens[1].trim();
        (StreetNames[streetName] || (StreetNames[streetName] = [])).push(suburbName); // several suburbs may exist for the same street name
    }
    StreetSuffixes = {};
    for (let line of fs.readFileSync("streetsuffixes.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let streetSuffixTokens = line.toUpperCase().split(",");
        StreetSuffixes[streetSuffixTokens[0].trim()] = streetSuffixTokens[1].trim();
    }
    SuburbNames = {};
    for (let line of fs.readFileSync("suburbnames.txt").toString().replace(/\r/g, "").trim().split("\n")) {
        let suburbTokens = line.toUpperCase().split(",");
        SuburbNames[suburbTokens[0].trim()] = suburbTokens[1].trim();
    }
    HundredNames = [];
    for (let line of fs.readFileSync("hundrednames.txt").toString().replace(/\r/g, "").trim().split("\n"))
        HundredNames.push(line.trim().toUpperCase());
    // Read the main page of development applications.
    let year = moment().format("YYYY");
    let developmentApplicationsUrl = DevelopmentApplicationsUrl.replace(/\{0\}/g, encodeURIComponent(year));
    console.log(`Retrieving page: ${developmentApplicationsUrl}`);
    let body = await request({ url: developmentApplicationsUrl, rejectUnauthorized: false, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);
    let $ = cheerio.load(body);
    let pdfUrls = [];
    for (let element of $("td.uContentListDesc a").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, developmentApplicationsUrl).href;
        if (pdfUrl.toLowerCase().includes(".pdf"))
            if (!pdfUrls.some(url => url === pdfUrl))
                pdfUrls.push(pdfUrl);
    }
    // Read the development applications page for another random year.
    let randomYear = getRandom(2008, moment().year() + 1).toString();
    let randomDevelopmentApplicationsUrl = DevelopmentApplicationsUrl.replace(/\{0\}/g, encodeURIComponent(randomYear));
    body = await request({ url: randomDevelopmentApplicationsUrl, rejectUnauthorized: false, proxy: process.env.MORPH_PROXY });
    await sleep(2000 + getRandom(0, 5) * 1000);
    $ = cheerio.load(body);
    let randomPdfUrls = [];
    for (let element of $("td.uContentListDesc a").get()) {
        let pdfUrl = new urlparser.URL(element.attribs.href, randomDevelopmentApplicationsUrl).href;
        if (pdfUrl.toLowerCase().includes(".pdf"))
            if (!randomPdfUrls.some(url => url === pdfUrl))
                randomPdfUrls.push(pdfUrl);
    }
    // Always parse the most recent PDF file and randomly select one other PDF file to parse.
    if (pdfUrls.length === 0 && randomPdfUrls.length === 0) {
        console.log("No PDF files were found on the pages examined.");
        return;
    }
    console.log(`Found ${pdfUrls.length + randomPdfUrls.length} PDF file(s).  Selecting two to parse.`);
    // Select the most recent PDF.  And randomly select one other PDF (avoid processing all PDFs
    // at once because this may use too much memory, resulting in morph.io terminating the current
    // process).
    let selectedPdfUrls = [];
    selectedPdfUrls.push(pdfUrls.shift()); // the most recent PDF
    if (randomPdfUrls.length > 0)
        selectedPdfUrls.push(randomPdfUrls[getRandom(0, randomPdfUrls.length)]); // a randomly selected PDF from a random year
    if (getRandom(0, 2) === 0)
        selectedPdfUrls.reverse();
    for (let pdfUrl of selectedPdfUrls) {
        console.log(`Parsing document: ${pdfUrl}`);
        let developmentApplications = await parsePdf(pdfUrl);
        console.log(`Parsed ${developmentApplications.length} development application(s) from document: ${pdfUrl}`);
        // Attempt to avoid reaching 512 MB memory usage (this will otherwise result in the
        // current process being terminated by morph.io).
        if (global.gc)
            global.gc();
        console.log(`Saving development applications to the database.`);
        for (let developmentApplication of developmentApplications)
            await insertRow(database, developmentApplication);
    }
}
main().then(() => console.log("Complete.")).catch(error => console.error(error));
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic2NyYXBlci5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbInNjcmFwZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUEsZ0dBQWdHO0FBQ2hHLGlDQUFpQztBQUNqQyxFQUFFO0FBQ0YsZUFBZTtBQUNmLHFCQUFxQjtBQUVyQixZQUFZLENBQUM7O0FBRWIseUJBQXlCO0FBQ3pCLG1DQUFtQztBQUNuQyxrREFBa0Q7QUFDbEQsbUNBQW1DO0FBQ25DLGlDQUFpQztBQUNqQyxpQ0FBaUM7QUFDakMsb0NBQW9DO0FBQ3BDLHlFQUFzRDtBQUV0RCxPQUFPLENBQUMsT0FBTyxFQUFFLENBQUM7QUFFbEIsTUFBTSwwQkFBMEIsR0FBRyx5REFBeUQsQ0FBQztBQUM3RixNQUFNLFVBQVUsR0FBRyxrQ0FBa0MsQ0FBQztBQUl0RCwyRUFBMkU7QUFFM0UsSUFBSSxXQUFXLEdBQUcsSUFBSSxDQUFDO0FBQ3ZCLElBQUksY0FBYyxHQUFHLElBQUksQ0FBQztBQUMxQixJQUFJLFdBQVcsR0FBRyxJQUFJLENBQUM7QUFDdkIsSUFBSSxZQUFZLEdBQUcsSUFBSSxDQUFDO0FBRXhCLDhCQUE4QjtBQUU5QixLQUFLLFVBQVUsa0JBQWtCO0lBQzdCLE9BQU8sSUFBSSxPQUFPLENBQUMsQ0FBQyxPQUFPLEVBQUUsTUFBTSxFQUFFLEVBQUU7UUFDbkMsSUFBSSxRQUFRLEdBQUcsSUFBSSxPQUFPLENBQUMsUUFBUSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1FBQ25ELFFBQVEsQ0FBQyxTQUFTLENBQUMsR0FBRyxFQUFFO1lBQ3BCLFFBQVEsQ0FBQyxHQUFHLENBQUMsd05BQXdOLENBQUMsQ0FBQztZQUN2TyxPQUFPLENBQUMsUUFBUSxDQUFDLENBQUM7UUFDdEIsQ0FBQyxDQUFDLENBQUM7SUFDUCxDQUFDLENBQUMsQ0FBQztBQUNQLENBQUM7QUFFRCxtRUFBbUU7QUFFbkUsS0FBSyxVQUFVLFNBQVMsQ0FBQyxRQUFRLEVBQUUsc0JBQXNCO0lBQ3JELE9BQU8sSUFBSSxPQUFP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