const puppeteer = require('puppeteer');
const mysql = require('mysql');

const exec = async () => {
    const browser = await puppeteer.launch({
        // headless: false
    });
    const page = await browser.newPage();
    await page.goto('https://coinmarketcap.com/rankings/exchanges/', {
        waitUntil: 'load',
        timeout: 0
    });

    await page.content()

    const obj = await page.evaluate(() => {
        const time = Date.now()
        const getExchange = (node) => {
            return {
                name: node.children[1].getElementsByTagName('p')[0].textContent,
                rank: node.children[0].children[0].textContent,
                exchangeScore: node.children[2].children[0] ? node.children[2].children[0].children[0].textContent : node.children[2].textContent,
                volume: node.children[3].getElementsByTagName('p')[0].textContent,
                quoteChange: `${node.children[3].getElementsByTagName('p')[1] ? (node.children[3].getElementsByTagName('p')[1].getAttribute('color') === 'red' ? '-' : '+') + node.children[3].getElementsByTagName('p')[1].textContent : ''}`,
                avgLiquidity: node.children[4].textContent,
                visitsSimilarweb: node.children[5].textContent,
                markets: node.children[6].textContent,
                coins: node.children[7].textContent,
                time,
            }
        }
        return {
            data: Array.from(document.getElementsByClassName('cmc-table')[0].getElementsByTagName('tbody')[0].children).map(item => getExchange(item))
        }
    })

    const connection = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        database: 'test'
    });

    connection.connect();
    connection.query(`CREATE TABLE IF NOT EXISTS data_1 (
        name VARCHAR(255) NOT NULL,
        rank INT NOT NULL,
        exchangeScore VARCHAR(255),
        volume VARCHAR(255),
        quoteChange VARCHAR(255),
        avgLiquidity VARCHAR(255),
        visitsSimilarweb VARCHAR(255),
        markets VARCHAR(255),
        coins INT,
        time BIGINT
    )`)

    connection.query(`CREATE TABLE IF NOT EXISTS current_table (
        id INT NOT NULL DEFAULT 1,
        current_table_id INT NOT NULL DEFAULT 1
    )`, (err, result) => {
        if (err) {
            throw (err)
        }
        if (!err) {
            console.log(result)
            connection.query(`SELECT COUNT(*) AS namesCount from current_table`, (err, result) => {
                if (err) throw err;
                if (result[0].namesCount === 0) {
                    connection.query(`INSERT INTO current_table (
                        current_table_id
                    ) VALUES(
                        1
                    )`)
                }
                connection.query(`SELECT * from current_table`, (err, result) => {
                    if (err) throw err;
                    let tableID = result[0].current_table_id
                    connection.query(`SELECT COUNT(*) AS namesCount from data_${tableID}`, (err, result) => {
                        if (err) throw err;
                        if (result[0].namesCount >= 100000) {
                            tableID = tableID + 1
                            connection.query(`UPDATE current_table SET current_table_id = ${tableID} WHERE id = 1`)
                            connection.query(`CREATE TABLE IF NOT EXISTS data_${tableID} (
                                name VARCHAR(255) NOT NULL,
                                rank INT NOT NULL,
                                exchangeScore VARCHAR(255),
                                volume VARCHAR(255),
                                quoteChange VARCHAR(255),
                                avgLiquidity VARCHAR(255),
                                visitsSimilarweb VARCHAR(255),
                                markets VARCHAR(255),
                                coins INT,
                                time BIGINT
                            )`)
                        }
                        obj.data.map(item => {
                            connection.query(`INSERT INTO data_${tableID} (name,rank,exchangeScore,volume,quoteChange,avgLiquidity,visitsSimilarweb,markets,coins,time) VALUES (?,?,?,?,?,?,?,?,?,?)`,
                                [item.name, item.rank, item.exchangeScore, item.volume, item.quoteChange, item.avgLiquidity, item.visitsSimilarweb, item.markets, Number(item.coins), item.time]
                            )
                        })
                    })
                })
            })
        }
    });

    // console.log(obj)

    await browser.close();
}

exec()

setInterval(exec, 3600 * 1000)