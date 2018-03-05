const util = require('util');
const mysql = require('mysql');
const request = require('request');
const express = require('express')
const yaml_config = require('node-yaml-config');
const Table = require('easy-table')

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

function matchModeName(mode) {
  switch (mode) {
    case 1:
      return 'solo';
    case 2:
      return 'duo';
    case 4:
      return 'squad';
  }

  return '?';
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

  var playerWasUpdated = false;

  if (checkStatsChanged(tkrStats.solo, soloStats)) {
    console.log('Updating player solo stats', {player: player});
    await storePlayerModeStatsDb(player, 1, tkrStats.solo);
    playerWasUpdated = true;
  }

  if (checkStatsChanged(tkrStats.duo, duoStats)) {
    console.log('Updating player duo stats', {player: player});
    await storePlayerModeStatsDb(player, 2, tkrStats.duo);
    playerWasUpdated = true;
  }

  if (checkStatsChanged(tkrStats.squad, squadStats)) {
    console.log('Updating player squad stats', {player: player});
    await storePlayerModeStatsDb(player, 4, tkrStats.squad);
    playerWasUpdated = true;
  }

  if (playerWasUpdated) {
    await updateMatches();
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


async function asyncQuery(query, params) {
  var args = [];
  for (var i = 0; i < arguments.length; ++i) {
    args.push(arguments[i]);
  }
  args.push(function(){});

  return new Promise((resolve, reject) => {
    args[args.length-1] = (err, results, fields) => {
      if (err) {
        reject(err);
        return;
      }

      resolve([results, fields]);
    }

    connection.query.apply(connection, args);
  });
}

function copyObject(obj) {
  var x = {};
  for (var i in obj) {
    if (obj.hasOwnProperty(i)) {
      x[i] = obj[i];
    }
  }
  return x;
}

function splitMatch(matchRow) {
  var matches = [];

  if (matchRow.matches > 1) {
    if (matchRow.top1 > 1 || matchRow.top3 > 1 ||
        matchRow.top5 > 1 || matchRow.top6 > 1 ||
        matchRow.top10 > 1 || matchRow.top12 > 1 ||
        matchRow.top25 > 1) {
      // Split the matches out via division

      var matchRemain = copyObject(matchRow);
      var perRowSub = {
        score: Math.ceil(matchRow.score / matchRow.matches),
        kills: Math.ceil(matchRow.kills / matchRow.matches),
        minsPlayed: Math.ceil(matchRow.minsPlayed / matchRow.matches),
        top1: Math.ceil(matchRow.top1 / matchRow.matches),
        top3: Math.ceil(matchRow.top3 / matchRow.matches),
        top5: Math.ceil(matchRow.top5 / matchRow.matches),
        top6: Math.ceil(matchRow.top6 / matchRow.matches),
        top10: Math.ceil(matchRow.top10 / matchRow.matches),
        top12: Math.ceil(matchRow.top12 / matchRow.matches),
        top25: Math.ceil(matchRow.top25 / matchRow.matches),
      };

      while (matchRemain.matches > 1) {
        var thisMatch = {
          statsid: matchRow.id,
          when: matchRow.when,
          player: matchRow.player,
          mode: matchRow.mode,
          score: Math.min(matchRemain.score, perRowSub.score),
          kills: Math.min(matchRemain.kills, perRowSub.kills),
          minsPlayed: Math.min(matchRemain.minsPlayed, perRowSub.minsPlayed),
          top1: Math.min(matchRemain.top1, perRowSub.top1),
          top3: Math.min(matchRemain.top3, perRowSub.top3),
          top5: Math.min(matchRemain.top5, perRowSub.top5),
          top6: Math.min(matchRemain.top6, perRowSub.top6),
          top10: Math.min(matchRemain.top10, perRowSub.top10),
          top12: Math.min(matchRemain.top12, perRowSub.top12),
          top25: Math.min(matchRemain.top25, perRowSub.top25),
          estim: 1
        };
        if (matchRow.top1 === -1) thisMatch.top1 = -1;
        if (matchRow.top3 === -1) thisMatch.top3 = -1;
        if (matchRow.top5 === -1) thisMatch.top5 = -1;
        if (matchRow.top6 === -1) thisMatch.top6 = -1;
        if (matchRow.top10 === -1) thisMatch.top10 = -1;
        if (matchRow.top12 === -1) thisMatch.top12 = -1;
        if (matchRow.top25 === -1) thisMatch.top25 = -1;
        matches.push(thisMatch);

        matchRemain.matches -= 1;
        matchRemain.score -= thisMatch.score;
        matchRemain.kills -= thisMatch.kills;
        matchRemain.minsPlayed -= thisMatch.minsPlayed;
        matchRemain.top1 -= thisMatch.top1;
        matchRemain.top3 -= thisMatch.top3;
        matchRemain.top5 -= thisMatch.top5;
        matchRemain.top6 -= thisMatch.top6;
        matchRemain.top10 -= thisMatch.top10;
        matchRemain.top12 -= thisMatch.top12;
        matchRemain.top25 -= thisMatch.top25;
      }

      var thisMatch = {
        statsid: matchRow.id,
        when: matchRow.when,
        player: matchRow.player,
        mode: matchRow.mode,
        matches: 1,
        score: matchRemain.score,
        kills: matchRemain.kills,
        minsPlayed: matchRemain.minsPlayed,
        top1: matchRemain.top1,
        top3: matchRemain.top3,
        top5: matchRemain.top5,
        top6: matchRemain.top6,
        top10: matchRemain.top10,
        top12: matchRemain.top12,
        top25: matchRemain.top25,
        estim: 1
      };
      if (matchRow.top1 === -1) thisMatch.top1 = -1;
      if (matchRow.top3 === -1) thisMatch.top3 = -1;
      if (matchRow.top5 === -1) thisMatch.top5 = -1;
      if (matchRow.top6 === -1) thisMatch.top6 = -1;
      if (matchRow.top10 === -1) thisMatch.top10 = -1;
      if (matchRow.top12 === -1) thisMatch.top12 = -1;
      if (matchRow.top25 === -1) thisMatch.top25 = -1;
      matches.push(thisMatch);
    } else {
      // Split the matches out as dead matches + good match
      var matchRemain = copyObject(matchRow);
      var perRowSub = {
        score: 25,
        kills: 0,
        minsPlayed: 1,
        top1: 0,
        top3: 0,
        top5: 0,
        top6: 0,
        top10: 0,
        top12: 0,
        top25: 0,
      };

      while (matchRemain.matches > 1) {
        var thisMatch = {
          statsid: matchRow.id,
          when: matchRow.when,
          player: matchRow.player,
          mode: matchRow.mode,
          score: Math.min(matchRemain.score, perRowSub.score),
          kills: Math.min(matchRemain.kills, perRowSub.kills),
          minsPlayed: Math.min(matchRemain.minsPlayed, perRowSub.minsPlayed),
          top1: Math.min(matchRemain.top1, perRowSub.top1),
          top3: Math.min(matchRemain.top3, perRowSub.top3),
          top5: Math.min(matchRemain.top5, perRowSub.top5),
          top6: Math.min(matchRemain.top6, perRowSub.top6),
          top10: Math.min(matchRemain.top10, perRowSub.top10),
          top12: Math.min(matchRemain.top12, perRowSub.top12),
          top25: Math.min(matchRemain.top25, perRowSub.top25),
          estim: 1
        };
        if (matchRow.top1 === -1) thisMatch.top1 = -1;
        if (matchRow.top3 === -1) thisMatch.top3 = -1;
        if (matchRow.top5 === -1) thisMatch.top5 = -1;
        if (matchRow.top6 === -1) thisMatch.top6 = -1;
        if (matchRow.top10 === -1) thisMatch.top10 = -1;
        if (matchRow.top12 === -1) thisMatch.top12 = -1;
        if (matchRow.top25 === -1) thisMatch.top25 = -1;
        matches.push(thisMatch);

        matchRemain.matches -= 1;
        matchRemain.score -= thisMatch.score;
        matchRemain.kills -= thisMatch.kills;
        matchRemain.minsPlayed -= thisMatch.minsPlayed;
        matchRemain.top1 -= thisMatch.top1;
        matchRemain.top3 -= thisMatch.top3;
        matchRemain.top5 -= thisMatch.top5;
        matchRemain.top6 -= thisMatch.top6;
        matchRemain.top10 -= thisMatch.top10;
        matchRemain.top12 -= thisMatch.top12;
        matchRemain.top25 -= thisMatch.top25;
      }

      var thisMatch = {
        statsid: matchRow.id,
        when: matchRow.when,
        player: matchRow.player,
        mode: matchRow.mode,
        matches: 1,
        score: matchRemain.score,
        kills: matchRemain.kills,
        minsPlayed: matchRemain.minsPlayed,
        top1: matchRemain.top1,
        top3: matchRemain.top3,
        top5: matchRemain.top5,
        top6: matchRemain.top6,
        top10: matchRemain.top10,
        top12: matchRemain.top12,
        top25: matchRemain.top25,
        estim: 1
      };
      if (matchRow.top1 === -1) thisMatch.top1 = -1;
      if (matchRow.top3 === -1) thisMatch.top3 = -1;
      if (matchRow.top5 === -1) thisMatch.top5 = -1;
      if (matchRow.top6 === -1) thisMatch.top6 = -1;
      if (matchRow.top10 === -1) thisMatch.top10 = -1;
      if (matchRow.top12 === -1) thisMatch.top12 = -1;
      if (matchRow.top25 === -1) thisMatch.top25 = -1;
      matches.push(thisMatch);
    }
  } else {
    // Match record is already correct
    var thisMatch = {
      statsid: matchRow.id,
      when: matchRow.when,
      player: matchRow.player,
      mode: matchRow.mode,
      matches: matchRow.matches,
      score: matchRow.score,
      kills: matchRow.kills,
      minsPlayed: matchRow.minsPlayed,
      top1: matchRow.top1,
      top3: matchRow.top3,
      top5: matchRow.top5,
      top6: matchRow.top6,
      top10: matchRow.top10,
      top12: matchRow.top12,
      top25: matchRow.top25,
      estim: 0
    };
    matches.push(thisMatch);
  }

  return matches;
}

async function updateMatches() {
  var maxMatchId = -1;
  const [maxmatches] = await asyncQuery('SELECT MAX(statsid) AS maxid FROM matches');
  if (maxmatches.length > 0 && maxmatches[0].maxid) {
    maxMatchId = maxmatches[0].maxid;
  }

  var q =
      'SELECT' +
      '  a.id,' +
      '  a.when `when`,' +
      '  a.player player,' +
      '  a.mode mode,' +
      '  a.matches-b.matches matches,' +
      '  a.score-b.score score,' +
      '  a.kills-b.kills kills,' +
      '  a.minsPlayed-b.minsPlayed minsPlayed,' +
      '  a.top1-b.top1 top1,' +
      '  a.top3-b.top3 top3,' +
      '  a.top5-b.top5 top5,' +
      '  a.top6-b.top6 top6,' +
      '  a.top10-b.top10 top10,' +
      '  a.top12-b.top12 top12,' +
      '  a.top25-b.top25 top25' +
      ' FROM stats a' +
      ' INNER JOIN stats b ON b.id=(SELECT MAX(c.id) FROM stats c WHERE c.player=a.player AND c.mode=a.mode AND c.id<a.id)' +
      ' WHERE a.id>?' +
      ' ORDER BY a.id ASC';
  const [matchData] = await asyncQuery(q, [maxMatchId]);

  for (var i = 0; i < matchData.length; ++i) {
    var rmatches = splitMatch(matchData[i]);

    for (var j = 0; j < rmatches.length; ++j) {
      var rmatch = rmatches[j];

      await asyncQuery('INSERT INTO matches(statsid,`when`,player,mode,score,kills,minsPlayed,top1,top3,top5,top6,top10,top12,top25,estim)' +
        ' VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
          rmatch.statsid,
          rmatch.when,
          rmatch.player,
          rmatch.mode,
          rmatch.score,
          rmatch.kills,
          rmatch.minsPlayed,
          rmatch.top1,
          rmatch.top3,
          rmatch.top5,
          rmatch.top6,
          rmatch.top10,
          rmatch.top12,
          rmatch.top25,
          rmatch.estim
        ]);
    }
  }
}

async function resetMatchCache() {
  await asyncQuery('TRUNCATE matches');
  await updateMatches();
}

const app = express();

async function fetchUserMatches(player) {
  return new Promise((resolve, reject) => {
    connection.query('SELECT * FROM matches WHERE player=? ORDER BY id DESC',
        [player], (err, results, fields) => {
          if (err) {
            reject(err);
            return;
          }

          var matches = [];
          for (var i = 0; i < results.length; ++i) {
            var row = {
              when: results[i].when,
              player: results[i].player,
              mode: results[i].mode,
              score: results[i].score,
              kills: results[i].kills,
              minsPlayed: results[i].minsPlayed,
              estim: results[i].estim,
            };

            switch (row.mode) {
              case 1:
                row.win = results[i].top1;
                row.top10 = results[i].top10;
                row.top25 = results[i].top25;
                break;
              case 2:
                row.win = results[i].top1;
                row.top5 = results[i].top5;
                row.top12 = results[i].top12;
                break;
              case 4:
                row.win = results[i].top1;
                row.top3 = results[i].top3;
                row.top6 = results[i].top6;
                break;
            }

            matches.push(row);
          }
          resolve(matches);
        });
  });
}

async function fetchUserData(player, mode) {
  return new Promise((resolve, reject) => {
    connection.query('SELECT * FROM stats WHERE player=? AND mode=? ORDER BY id DESC',
        [player, mode],
        (err, results, fields) => {
          if (err) {
            reject(err);
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

          resolve(out);
        });
  });
}

app.get('/api/recalc_matches', (req, res) => {
  resetMatchCache().then(() => {
    res.send({});
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/clear_cache', (req, res) => {
  STATS_CACHE = {};
  res.send({});
});

app.get('/api/user/:player/matches', (req, res) => {
  fetchUserMatches(req.params.player).then((matches) => {
    res.send({matches: matches});
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/user/:player/matches/table', (req, res) => {
  fetchUserMatches(req.params.player).then((matches) => {
    var t = new Table();
    matches.forEach(function(match) {
      t.cell('When', match.when);
      t.cell('Mode', matchModeName(match.mode));
      t.cell('Score', match.score);
      t.cell('Kills', match.kills);
      t.cell('Mins Played', match.minsPlayed);
      switch (match.mode) {
        case 1:
          t.cell('Win', match.top1);
          t.cell('Top 3', '');
          t.cell('Top 5', '');
          t.cell('Top 6', '');
          t.cell('Top 10', match.top10);
          t.cell('Top 12', '');
          t.cell('Top 25', match.top25);
          break;
        case 2:
          t.cell('Win', match.top1);
          t.cell('Top 3', '');
          t.cell('Top 5', match.top5);
          t.cell('Top 6', '');
          t.cell('Top 10', '');
          t.cell('Top 12', match.top12);
          t.cell('Top 25', '');
          break;
        case 4:
          t.cell('Win', match.top1);
          t.cell('Top 3', match.top3);
          t.cell('Top 5', '');
          t.cell('Top 6', match.top6);
          t.cell('Top 10', '');
          t.cell('Top 12', '');
          t.cell('Top 25', '');
          break;
      }
      t.cell('Estimated?', match.estim);
      t.newRow()
    });
    res.send(t.toString());
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/user/:player/solo', (req, res) => {
  fetchUserData(req.params.player, 1).then((data) => {
    res.send({
      data: data
    });
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/user/:player/duo', (req, res) => {
  fetchUserData(req.params.player, 2).then((data) => {
    res.send({
      data: data
    });
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/user/:player/squad', (req, res) => {
  fetchUserData(req.params.player, 4).then((data) => {
    res.send({
      data: data
    });
  }).catch((err) => {
    res.send({error: err});
  });
});

app.get('/api/user/:player', (req, res) => {
  res.send(USER_INFO[req.params.player]);
});

app.get('/api/users', (req, res) => {
  res.send(USER_INFO);
});


app.listen(LISTEN_PORT, () => console.log('API listening on port ' + LISTEN_PORT + '!'))
