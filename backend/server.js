const express = require("express");
const mysql = require("mysql2");
const cors = require("cors");

const app = express();
app.use(cors());

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "pr@tham262003",
  database: "Football_Stats",
});

db.connect((err) => {
  if (err) {
    console.error("Database connection error:", err);
    return;
  }
  console.log("Connected to the database");
});

app.get("/", (req, res) => {
  return res.json("From Backend Side");
});

app.get("/player/search", (req, res) => {
  const name = req.query.name; // Get the search query from the request query parameters
  const sql = "SELECT * FROM player_stats WHERE P_Name = ?";

  db.query(sql, [name], (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/player", (req, res) => {
  const sql = "SELECT * FROM player_stats WHERE P_Name= 'Lionel Messi' ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/laliga", (req, res) => {
  const sql = "SELECT * FROM points_table LIMIT 20 ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/pl", (req, res) => {
  const sql = "SELECT * FROM points_table LIMIT 20 OFFSET 20 ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/bundesliga", (req, res) => {
  const sql = "SELECT * FROM points_table LIMIT 18 OFFSET 40 ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/ligue1", (req, res) => {
  const sql = "SELECT * FROM points_table LIMIT 18 OFFSET 58 ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/seriea", (req, res) => {
  const sql = "SELECT * FROM points_table LIMIT 20 OFFSET 78 ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/goals", (req, res) => {
  const sql = "SELECT * FROM Goals ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/assists", (req, res) => {
  const sql = "SELECT * FROM Assists ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/clean", (req, res) => {
  const sql = "SELECT * FROM Clean_Sheets ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/InterStats", (req, res) => {
  const sql = "SELECT * FROM Inter_Stats ";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/InterClub", (req, res) => {
  const sql =
    "SELECT S.Rank, S.Player, S.Country, S.Club_Name, S.Goals, M.MANAGER AS Manager FROM Inter_Club_Tour_Stats S JOIN manager M ON S.Club_Name = M.Club_Name";

  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

app.get("/rank", (req, res) => {
  const sql = "SELECT * FROM RK";
  db.query(sql, (err, data) => {
    if (err) throw err;
    return res.json(data);
  });
});

// Port of server
app.listen(8081, () => {
  console.log(`Server is running on port ${8081}`);
});
