const util = require('util');
const mysql = require('mysql');
const request = require('request');
const express = require('express')
const yaml_config = require('node-yaml-config');

const config = yaml_config.load(__dirname + '/config.yml');

if (!config.players.length) {
  console.error('no players configured to poll');
}

if (!config.polling.total) {
  console.error('no poll period configured');
}

var TRN_KEY = config.trn.apikey;
var LISTEN_PORT = config.listen.port;
var USERS = config.players;
var POLL_PERIOD = Math.ceil(config.polling.total / USERS.length);
console.log('Poll period set to the following per user:', POLL_PERIOD);

var USER_INFO = {};
for (var i = 0; i < USERS.length; ++i) {
  var player = USERS[i];
  USER_INFO[player] = {
    lastUpdated: null
  };
}

var connection = mysql.createConnection({
  host: config.db.host,
  user: config.db.user,
  password: config.db.password,
  database: config.db.database
});
connection.connect();

setInterval(() => {
  connection.ping();
}, 60000);

function blankStats() {
  return {
    score: -1,
    top1: -1,
    top3: -1,
    top5: -1,
    top6: -1,
    top10: -1,
    top12: -1,
    top25: -1,
    matches: -1,
    kills: -1,
    minsPlayed: -1
  };
}

function checkStatsChanged(s1, s2) {
  return s1.score != s2.score ||
         s1.top1 != s2.top1 ||
         s1.top3 != s2.top3 ||
         s1.top5 != s2.top5 ||
         s1.top6 != s2.top6 ||
         s1.top10 != s2.top10 ||
         s1.top12 != s2.top12 ||
         s1.top25 != s2.top25 ||
         s1.matches != s2.matches ||
         s1.kills != s2.kills ||
         s1.minsPlayed != s2.minsPlayed;
}

async function fetchPlayerStatsTkr(player) {
  return new Promise((resolve, reject) => {
    request({
      uri: 'https://api.fortnitetracker.com/v1/profile/pc/' + player,
      headers: {
        'TRN-Api-Key': TRN_KEY
      }
    }, (err, response, body) => {
      if (err) {
        reject(err);
        return;
      }

      var tkrStats = JSON.parse(body);

      var statsOut = {
        solo: blankStats(),
        duo: blankStats(),
        squad: blankStats()
      };

      if (tkrStats.stats.p2) {
        statsOut.solo.score = tkrStats.stats.p2.score.valueInt;
        statsOut.solo.top1 = tkrStats.stats.p2.top1.valueInt;
        statsOut.solo.top10 = tkrStats.stats.p2.top10.valueInt;
        statsOut.solo.top25 = tkrStats.stats.p2.top25.valueInt;
        statsOut.solo.matches = tkrStats.stats.p2.matches.valueInt;
        statsOut.solo.kills = tkrStats.stats.p2.kills.valueInt;
        statsOut.solo.minsPlayed = tkrStats.stats.p2.minutesPlayed.valueInt;
      }

      if (tkrStats.stats.p10) {
        statsOut.duo.score = tkrStats.stats.p10.score.valueInt;
        statsOut.duo.top1 = tkrStats.stats.p10.top1.valueInt;
        statsOut.duo.top5 = tkrStats.stats.p10.top5.valueInt;
        statsOut.duo.top12 = tkrStats.stats.p10.top12.valueInt;
        statsOut.duo.matches = tkrStats.stats.p10.matches.valueInt;
        statsOut.duo.kills = tkrStats.stats.p10.kills.valueInt;
        statsOut.duo.minsPlayed = tkrStats.stats.p10.minutesPlayed.valueInt;
      }

      if (tkrStats.stats.p9) {
        statsOut.squad.score = tkrStats.stats.p9.score.valueInt;
        statsOut.squad.top1 = tkrStats.stats.p9.top1.valueInt;
        statsOut.squad.top3 = tkrStats.stats.p9.top3.valueInt;
        statsOut.squad.top6 = tkrStats.stats.p9.top6.valueInt;
        statsOut.squad.matches = tkrStats.stats.p9.matches.valueInt;
        statsOut.squad.kills = tkrStats.stats.p9.kills.valueInt;
        statsOut.squad.minsPlayed = tkrStats.stats.p9.minutesPlayed.valueInt;
      }

      resolve(statsOut);
    })
  });
}

async function fetchPlayerModeStatsDb(player, mode) {
  return new Promise((resolve, reject) => {
    connection.query('SELECT * FROM stats WHERE player=? AND mode=? ORDER BY id DESC LIMIT 1', [player, mode], function (err, results, fields) {
      if (err) {
        reject(err);
        return;
      }

      if (results.length === 0) {
        resolve(blankStats());
        return;
      }

      resolve({
        score: results[0].score,
        top1: results[0].top1,
        top3: results[0].top3,
        top5: results[0].top5,
        top6: results[0].top6,
        top10: results[0].top10,
        top12: results[0].top12,
        top25: results[0].top25,
        matches: results[0].matches,
        kills: results[0].kills,
        minsPlayed: results[0].minsPlayed,
      });
    });
  });
}

var STATS_CACHE = {};

async function storePlayerModeStatsDb(player, mode, stats) {
  var statsKey = player + '/' + mode;
  STATS_CACHE[statsKey] = stats;

  return new Promise((resolve, reject) => {
    connection.query('INSERT INTO stats(player,mode,score,top1,top3,top5,top6,top10,top12,top25,matches,kills,minsPlayed) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [player, mode,
        stats.score,
        stats.top1,
        stats.top3,
        stats.top5,
        stats.top6,
        stats.top10,
        stats.top12,
        stats.top25,
        stats.matches,
        stats.kills,
        stats.minsPlayed
      ], function (err, results, fields) {
        if (err) {
          reject(err);
          return;
        }

        resolve(true);
      });
  });
}

async function fetchPlayerModeStats(player, mode) {
  var statsKey = player + '/' + mode;
  if (STATS_CACHE[statsKey]) {
    return STATS_CACHE[statsKey];
  }

  var stats = await fetchPlayerModeStatsDb(player, mode);
  STATS_CACHE[statsKey] = stats;

  return stats;
};

async function maybeUpdatePlayer(player, callback) {
  USER_INFO[player].lastUpdated = new Date();

  var tkrStats = await fetchPlayerStatsTkr(player);

  var soloStats = await fetchPlayerModeStats(player, 1);
  var duoStats = await fetchPlayerModeStats(player, 2);
  var squadStats = await fetchPlayerModeStats(player, 4);

  if (checkStatsChanged(tkrStats.solo, soloStats)) {
    console.log('Updating player solo stats', {player: player});
    await storePlayerModeStatsDb(player, 1, tkrStats.solo);
  }

  if (checkStatsChanged(tkrStats.duo, duoStats)) {
    console.log('Updating player duo stats', {player: player});
    await storePlayerModeStatsDb(player, 2, tkrStats.duo);
  }

  if (checkStatsChanged(tkrStats.squad, squadStats)) {
    console.log('Updating player squad stats', {player: player});
    await storePlayerModeStatsDb(player, 4, tkrStats.squad);
  }
}

var currentPlayerIdx = 0;
function updateNextPlayer() {
  var currentPlayer = USERS[currentPlayerIdx];

  maybeUpdatePlayer(currentPlayer)
      .then(() => {
        console.log('Player updated', {player: currentPlayer});
      }).catch((err) => {
        console.log('Failed to update player', {error: err});
      });

  currentPlayerIdx++;
  if (currentPlayerIdx >= USERS.length) {
    currentPlayerIdx = 0;
  }

  setTimeout(function() {
    updateNextPlayer();
  }, POLL_PERIOD);
}
updateNextPlayer();



const app = express()

function handleUserStatsReq(player, mode, res) {
  connection.query('SELECT * FROM stats WHERE player=? AND mode=? ORDER BY id DESC',
      [player, mode],
      function (err, results, fields) {
        if (err) {
          res.send({
            error: err
          });
          return;
        }

        var out = [];
        for (var i = 0; i < results.length; ++i) {
          var row = {};
          row.fetchedAt = results[i].when;
          row.score = results[i].score;
          row.matches = results[i].matches;
          row.kills = results[i].kills;
          row.minsPlayed = results[i].minsPlayed;

          switch (mode) {
            case 1:
              row.wins = results[i].top1;
              row.top10 = results[i].top10;
              row.top25 = results[i].top25;
              break;
            case 2:
              row.wins = results[i].top1;
              row.top5 = results[i].top5;
              row.top12 = results[i].top12;
              break;
            case 4:
              row.wins = results[i].top1;
              row.top3 = results[i].top3;
              row.top6 = results[i].top6;
              break;
          }

          out.push(row);
        }

        res.send({
          player: USER_INFO[player],
          data: out
        });
      });
}

app.get('/api/user/:player/solo', (req, res) => {
  handleUserStatsReq(req.params.player, 1, res);
});

app.get('/api/user/:player/duo', (req, res) => {
  handleUserStatsReq(req.params.player, 2, res);
});

app.get('/api/user/:player/squad', (req, res) => {
  handleUserStatsReq(req.params.player, 4, res);
});

app.get('/api/user/:player', (req, res) => {
  res.send(USER_INFO[req.params.player]);
});

app.get('/api/users', (req, res) => {
  res.send(USER_INFO);
});


app.listen(LISTEN_PORT, () => console.log('API listening on port ' + LISTEN_PORT + '!'))
