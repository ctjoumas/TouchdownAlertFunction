namespace PlayAlertFunction
{
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus;
    using HtmlAgilityPack;
    using Microsoft.AspNetCore.Http;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections;
    using System.Data.SqlClient;
    using System.IO;
    using System.Threading.Tasks;

    public class TouchdownAlert
    {
        /// <summary>
        /// Session key for the Azure SQL Access token
        /// </summary>
        public const string SessionKeyAzureSqlAccessToken = "_Token";

        /// <summary>
        /// Root of the play by play URL where we will get the play by play json object
        /// </summary>
        private const string PLAY_BY_PLAY_URL = "https://www.espn.com/nfl/playbyplay/_/gameId/";

        private const int RECEIVING_AND_RUSHING_BIG_PLAY_YARDAGE = 25;
        private const int PASSING_BIG_PLAY_YARDAGE = 40;

        public TouchdownAlert()
        {
        }

        [FunctionName("ParseTouchdownsFromJson")]
        public async void RunTouchdownTest([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log)
        {
            string requestBody = String.Empty;

            using (StreamReader streamReader = new StreamReader(req.Body))
            {
                try
                {
                    string jsonString = JsonConvert.SerializeObject(streamReader.ReadToEnd());
                    log.LogInformation(jsonString);
                    JObject jsonPlayByPlayDoc = JObject.Parse(jsonString);

                    string value = ((JValue)jsonPlayByPlayDoc.SelectToken("drives.previous[0].displayResult")).Value.ToString();
                    log.LogInformation(value);

                    ParseSinglePlayerTouchdownTest(jsonPlayByPlayDoc, "David Blough", log);
                }
                catch (Exception e)
                {
                    log.LogInformation(e.Message);
                }
            }
        }

        [FunctionName("ParseBigPlayFromJson")]
        public async void RunBigPlayTest([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, ILogger log, ExecutionContext context)
        {
            string requestBody = String.Empty;

            using (StreamReader streamReader = new StreamReader(req.Body))
            {
                try
                {
                    string jsonString = JsonConvert.SerializeObject(streamReader.ReadToEnd());
                    jsonString = jsonString.Substring(1, jsonString.Length - 2);
                    jsonString.Replace(@"\r", @"");
                    jsonString.Replace(@"\n", @"");
                    jsonString.Replace(@"\", @"");
                    
                    log.LogInformation(jsonString);
                    JObject jsonPlayByPlayDoc = JObject.Parse(jsonString);
                    
                    //string value = ((JValue)jsonPlayByPlayDoc.SelectToken("drives.current.displayResult")).Value.ToString();
                    //log.LogInformation(value);

                    ParseSinglePlayerBigPlayTest(jsonPlayByPlayDoc, "Deebo Samuel", log, context);
                }
                catch (Exception e)
                {
                    log.LogInformation(e.Message);
                }
            }
        }

        private void ParseSinglePlayerTouchdownTest(JObject playByPlayJsonObject, string playerName, ILogger log)
        {
            // each play token is a drive, so we will go through this to parse all player stats
            JToken driveTokens = playByPlayJsonObject.SelectToken("drives.previous");

            // if the game started and there are no drives yet
            if (driveTokens != null)
            {
                foreach (JToken driveToken in driveTokens)
                {
                    JToken driveResultValue = driveToken.SelectToken("displayResult");

                    if (driveResultValue != null)
                    {
                        // if a touchdown is scored, the text will be "Touchdown"
                        string driveResult = ((JValue)driveToken.SelectToken("displayResult")).Value.ToString();

                        if (driveResult == null)
                        {
                            log.LogInformation("Drive Result is NULL! Need to check why...");
                        }

                        // only parse the plays in this drive if this drive resulted in a made touchdown
                        if (driveResult.ToLower().Equals(("touchdown")))
                        {
                            // get the number of plays
                            int numPlays = ((JArray)driveToken.SelectToken("plays")).Count;

                            // the last node of the plays node will have the scoring play
                            JToken playToken = driveToken.SelectToken("plays[" + (numPlays - 1) + "]");

                            // get the details of the touchdown
                            // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                            // send a message to the service bus that the same touchdown was scored
                            int quarter = int.Parse(playToken.SelectToken("period.number").ToString());
                            string gameClock = (string)((JValue)playToken.SelectToken("clock.displayValue")).Value;

                            // the player name is displayed here, but it's usually first initial.lastname (G.Kittle), so we'd
                            // search for this player name in the players table for the current roster / matchup
                            string touchdownText = (string)((JValue)playToken.SelectToken("text")).Value;

                            // get the player name as first <initial>.<lastname> to check if this is the player
                            // who scored a touchdown
                            string abbreviatedPlayerName = playerName;
                            int spaceIndex = abbreviatedPlayerName.IndexOf(' ');
                            abbreviatedPlayerName = abbreviatedPlayerName[0] + "." + abbreviatedPlayerName.Substring(spaceIndex + 1);

                            if (touchdownText.Contains(abbreviatedPlayerName) && (touchdownText.IndexOf("TOUCHDOWN") <= touchdownText.IndexOf(abbreviatedPlayerName)))
                            {
                                log.LogInformation("Did NOT add TD for " + playerName + "; Player is a kicker.");
                            }

                            // We need to make sure that this player is the player who scored the TD and not the kicker kicking the XP. The
                            // format of the text in the JSON Play By Play will be:
                            // "text": "(5:30) (Shotgun) D.Samuel left end for 8 yards, TOUCHDOWN. R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                            // It should be enough to ensure the occurence of the player has to be before the occurence of the text "TOUCHDOWN"
                            if (touchdownText.Contains(abbreviatedPlayerName) && (touchdownText.IndexOf("TOUCHDOWN") > touchdownText.IndexOf(abbreviatedPlayerName)))
                            {
                                log.LogInformation("Added TD for " + playerName);
                            }
                        }
                    }
                }
            }
        }

        private async void ParseSinglePlayerBigPlayTest(JObject playByPlayJsonObject, string playerName, ILogger log, ExecutionContext context)
        {
            // get all plays in the current drive
            JToken playTokens = playByPlayJsonObject.SelectToken("drives.current.plays");

            // used to determine if a big play occured, whether it's passing, receiving, or rushing
            bool bigPlayOccurred = false;

            // if the game started and there are plays in the current drive
            if (playTokens != null)
            {
                foreach (JToken playToken in playTokens)
                {
                    // we can get the yardage of the play from the statYardage property
                    int playYardage = (int)Int64.Parse(((JValue)playToken.SelectToken("statYardage")).Value.ToString());

                    // a passing or receiving play requires less yardage than a pass play to be considered a big play,
                    // so that is our minimum threshold for a big play; if we don't have that,w e can skip this play
                    // we can get the yardage of the play from the statYardage property
                    if (playYardage >= RECEIVING_AND_RUSHING_BIG_PLAY_YARDAGE)
                    {
                        // get the details of the touchdown
                        // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                        // send a message to the service bus that the same touchdown was scored
                        int quarter = int.Parse(playToken.SelectToken("period.number").ToString());
                        string gameClock = (string)((JValue)playToken.SelectToken("clock.displayValue")).Value;

                        // the player name is displayed here, but it's usually first initial.lastname (G.Kittle), so we'd
                        // search for this player name in the players table for the current roster / matchup
                        string bigPlayText = (string)((JValue)playToken.SelectToken("text")).Value;

                        // get the player name as first <initial>.<lastname> to check if this is the player
                        // who scored a touchdown
                        string abbreviatedPlayerName = playerName;
                        int spaceIndex = abbreviatedPlayerName.IndexOf(' ');
                        abbreviatedPlayerName = abbreviatedPlayerName[0] + "." + abbreviatedPlayerName.Substring(spaceIndex + 1);

                        PlayDetails playDetails = new PlayDetails();
                        playDetails.OwnerId = 2;
                        playDetails.OwnerName = "Chris";
                        playDetails.PhoneNumber = "703-436-6826";
                        playDetails.PlayerName = playerName;
                        playDetails.Season = 2022;

                        // if this player was involved in the play, let's determine the type of play
                        if (bigPlayText.Contains(abbreviatedPlayerName))
                        {
                            string playType = ((JValue)playToken.SelectToken("type.abbreviation")).ToString();
                            bool passingPlay = playType.ToLower().Equals("rec") ? true : false;

                            // If this is a pass play, we need to determine if this player threw the ball or received it
                            if (passingPlay)
                            {
                                // If the occurence of the word "pass" occurs after the player name, then this player threw the pass;
                                // otherwise, the player received it
                                if (bigPlayText.IndexOf(abbreviatedPlayerName) < bigPlayText.IndexOf("pass"))
                                {
                                    // player threw a pass, so we'll only alert if it's above the passing yardage threshold
                                    if (playYardage >= PASSING_BIG_PLAY_YARDAGE)
                                    {
                                        bigPlayOccurred = true;
                                        playDetails.Message = "Big play! " + playDetails.PlayerName + " threw a pass of " + playYardage;
                                    }
                                }
                                else
                                {
                                    bigPlayOccurred = true;

                                    // player received a pass, and we already know it's above the threshold since that was our
                                    // first check, so just send the alert
                                    playDetails.Message = "Big play! " + playDetails.PlayerName + " caught a pass of " + playYardage;
                                }
                            }
                            else
                            {
                                bigPlayOccurred = true;
                                playDetails.Message = "Big play! " + playDetails.PlayerName + " rushed for " + playYardage;
                            }
                        }

                        // if a big play occurred, let's add it to the database
                        if (bigPlayOccurred)
                        {
                            // if this big play by this player was not already parsed, the big play will be added
                            bool bigPlayAdded = AddBigPlayDetails(0, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                            if (bigPlayAdded)
                            {
                                var configurationBuilder = new ConfigurationBuilder()
                                .SetBasePath(context.FunctionAppDirectory)
                                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                                .AddEnvironmentVariables()
                                .Build();

                                log.LogInformation("Added big play for " + playDetails.PlayerName);
                                await sendPlayMessage(playDetails, configurationBuilder);
                            }
                            else
                            {
                                log.LogInformation("Did NOT log big play for " + playDetails.PlayerName + "; big play already parsed earlier.");
                            }
                        }

                        // reset the flag as there could be another player involved in the same play
                        bigPlayOccurred = false;
                    }
                }
            }
        }




        // http://crontab.cronhub.io/?msclkid=5dd54af5c24911ecad1f7dea98c7030e to verify timer triggers
        // The timer trigger should run every 10 seconds on Sundays from 1-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} * is every minute
        // {hour} 13-23 is 1pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-1 is Sept-Jan
        // {day of week} 0 is Sunday
        [FunctionName("ParseTouchdownsSunday")]
        public void RunSunday([TimerTrigger("*/10 * 9-23 * 9-12 0")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Sunday games at " + DateTime.Now);

            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            //string serviceBusSharedAccessSignature = configurationBuilder["ServiceBusSharedAccessKey"];
            //log.LogInformation("Found SB SAS - original function: " + serviceBusSharedAccessSignature);

            Hashtable gamesToParse = getGamesToParse(log);

            parseTouchdownsAndBigPlays(gamesToParse, log, configurationBuilder);
        }

        // The timer trigger should run every 10 seconds on Thursdays from 8-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} 20 is at 20 minutes past the hour (thursday night games start at 8:20) - using *
        // {hour} 20-23 is 8pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-12 is Sept-Dec
        // {day of week} 4 is Thursday
        [FunctionName("ParseTouchdownsThursday")]
        public void RunThursday([TimerTrigger("*/10 * 20-23 * 9-12 4")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Thursday games at " + DateTime.Now);

            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            Hashtable gamesToParse = getGamesToParse(log);

            parseTouchdownsAndBigPlays(gamesToParse, log, configurationBuilder);
        }

        // TESTING PRESEASON GAME
        // ======================
        // The timer trigger should run every 10 seconds on Thursdays from 8-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} * is starting at the hour specified below (these preseason games start at 1pm)
        // {hour} 16-20 is 4pm-8pm
        // {day of the month} * is every day
        // {month} 8 is Aug
        // {day of week} 7 is Sunday
        [FunctionName("ParseTouchdownsSaturdayPreseason")]
        public void RunSaturdayPreseason([TimerTrigger("*/10 * 15-23 * 8 6")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Saturday Week 3 preseason games at " + DateTime.Now);

            var configurationBuilder = new ConfigurationBuilder()
                            .SetBasePath(context.FunctionAppDirectory)
                            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                            .AddEnvironmentVariables()
                            .Build();

            Hashtable gamesToParse = getGamesToParse(log);

            parseTouchdownsAndBigPlays(gamesToParse, log, configurationBuilder);
        }


        // The timer trigger should run every 10 seconds on Mondays from 8pm-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} 15 is at every 15 minutes past the hours (monday night games start at 8:15)
        // {hour} 20-23 is 8pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-1 is Sept-Jan
        // {day of week} 1 is Monday
        [FunctionName("ParseTouchdownsMonday")]
        public void RunMonday([TimerTrigger("*/10 * 20-23 * 9-12 1")] TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Monday games at " + DateTime.Now);

            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string serviceBusSharedAccessSignature = configurationBuilder["ServiceBusSharedAccessKey"];
            log.LogInformation("Found SB SAS - original function: " + serviceBusSharedAccessSignature);

            Hashtable gamesToParse = getGamesToParse(log);

            parseTouchdownsAndBigPlays(gamesToParse, log, configurationBuilder);
        }

        /// <summary>
        /// Gets the rosters for the latest/current week from the CurrentRoster table and stores it
        /// in a hashtable where the key is the ESPN Game ID and the value are all players playing
        /// in that game.
        /// </summary>
        /// <param name="log">Logger</param>
        /// <returns>Hashtable of games for each player from the current weeks roster</returns>
        private Hashtable getGamesToParse(ILogger log)
        {
            Hashtable gamesToParse = new Hashtable();

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandscheduledetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to get all players for each team's roster for this week
                using (SqlCommand command = new SqlCommand("GetTeamsForCurrentWeek", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            int season = (int)reader.GetValue(reader.GetOrdinal("Season"));
                            int ownerId = (int)reader.GetValue(reader.GetOrdinal("OwnerID"));
                            string ownerName = reader.GetValue(reader.GetOrdinal("OwnerName")).ToString();
                            string ownerPhoneNumber = reader.GetValue(reader.GetOrdinal("PhoneNumber")).ToString();
                            string playerName = reader.GetValue(reader.GetOrdinal("PlayerName")).ToString();
                            string teamAbbreviation = reader.GetValue(reader.GetOrdinal("TeamAbbreviation")).ToString();
                            string opponentAbbreviation = reader.GetValue(reader.GetOrdinal("OpponentAbbreviation")).ToString();
                            bool gameEnded = (bool)reader.GetValue(reader.GetOrdinal("GameEnded"));
                            DateTime gameDate = DateTime.Parse((reader.GetValue(reader.GetOrdinal("GameDate")).ToString()));
                            string espnGameId = reader.GetValue(reader.GetOrdinal("EspnGameId")).ToString();

                            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
                            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);
                            TimeSpan difference = gameDate.Subtract(currentEasterStandardTime);

                            // if the game hasn't started or the game has ended, don't load the HtmlDoc to parse stats since we've already done that
                            if ((difference.TotalDays < 0) && (!gameEnded))
                            {
                                PlayDetails playDetails = new PlayDetails();
                                playDetails.Season = season;
                                playDetails.OwnerId = ownerId;
                                playDetails.OwnerName = ownerName;
                                playDetails.PhoneNumber = ownerPhoneNumber;
                                playDetails.TeamAbbreviation = teamAbbreviation;
                                playDetails.OpponentAbbreviation = opponentAbbreviation;
                                playDetails.GameDate = gameDate;
                                playDetails.PlayerName = playerName;

                                // it's more expensive to use the ContainsKey method on a hashtable, so just pull out
                                // the value and check if it's null
                                ArrayList playerList = (ArrayList)gamesToParse[espnGameId];

                                // if it's not null, the game exists in the hashtable, so let's remove the item so we can add the
                                // touchdown details for this player to the list and re-add the key/value pair with this new player's
                                // touchdown details. Oherwise, we will create an empty ArrayList for the players touchdown details so
                                // we can add the touchdown details and put the new game key/value pair into the hashtable
                                if (playerList != null)
                                {
                                    gamesToParse.Remove(espnGameId);
                                }
                                else
                                {
                                    playerList = new ArrayList();
                                }

                                playerList.Add(playDetails);
                                gamesToParse.Add(espnGameId, playerList);

                                log.LogInformation("player name: " + playerName + "(" + ownerPhoneNumber + ")");
                            }
                        }
                    }
                }

                sqlConnection.Close();
            }

            return gamesToParse;
        }

        /// <summary>
        /// Parses touchdowns for each game that each player in the active rosters for both owners are playing in.
        /// </summary>
        /// <param name="gamesToParse">Key is the Espn Game ID and the value is the list of players playing in the game</param>
        /// <param name="log">Logger</param>
        public void parseTouchdownsAndBigPlays(Hashtable gamesToParse, ILogger log, IConfiguration configurationBuilder)
        {
            JObject playByPlayJsonObject;

            // go through each key (game id) in the hashtable and parse these games' JSON play
            // by play checking for each player (value, which is a list of players) playing in
            // that game
            foreach (var key in gamesToParse.Keys)
            {
                log.LogInformation("Key for play by play URL is: " + key);

                string playByPlayUrl = PLAY_BY_PLAY_URL + key;
                HtmlDocument playByPlayDoc = new HtmlWeb().Load(playByPlayUrl);

                // get the play by play JSON object for this game
                playByPlayJsonObject = GetPlayByPlayJsonObject(playByPlayDoc, log);

                if (playByPlayJsonObject == null)
                {
                    log.LogInformation("Play by play JSON object is NULL! at " + DateTime.Now);
                }
                else
                {
                    log.LogInformation("Play by play JSON is good at " + DateTime.Now);

                    ArrayList playersInGame = (ArrayList)gamesToParse[key];

                    //ParsePlayerTouchdownsForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, log, configurationBuilder);
                    ParsePlayerBigPlaysAndTouchdownsForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, log, configurationBuilder);
                }
            }
        }

        /// <summary>
        /// When a game is in progress, the play by play data is updated in the javascript function's espn.gamepackage.data variable.
        /// This method will find this variable and pull out the JSON and store it so we can parse the live play by play data.
        /// </summary>
        /// <param name="playByPlayDoc">The play by play document for a particular game</param>
        /// <returns>The JSON object representing the play by play data.</returns>
        public JObject GetPlayByPlayJsonObject(HtmlDocument playByPlayDoc, ILogger log)
        {
            JObject playByPlayJsonObject = null;

            var playByPlayJavaScriptNode = playByPlayDoc.DocumentNode.SelectNodes("//script[@type='text/javascript']");

            foreach (var scriptNode in playByPlayJavaScriptNode)
            {
                if (scriptNode.InnerText.Contains("espn.gamepackage.data"))
                {
                    string[] javascriptLines = scriptNode.InnerText.Split('\n');

                    foreach (var line in javascriptLines)
                    {
                        if (line.Contains("espn.gamepackage.data"))
                        {
                            string variable = line.Trim();

                            variable = variable.Substring(variable.IndexOf("{"));

                            // there is a trailing ;, so pull that off
                            variable = variable.Substring(0, variable.Length - 1);

                            // load into JSON object
                            playByPlayJsonObject = JObject.Parse(variable);

                            log.LogInformation("Got live JSON object for game.");

                            break;
                        }
                    }

                    break;
                }
            }

            return playByPlayJsonObject;
        }            

        /// <summary>
        /// Parses the current drive of the JSON object for the given game to see if any of the players
        /// playing in this game just got a big play or a touchdown. If the touchdown has not yet been
        /// texted to the owner, based on game clock stored as last parsed touchdown in the database, a
        /// text will be sent to the owner.
        /// </summary>
        /// <param name="playByPlayJsonObject"></param>
        /// <param name="playersInGame"></param>
        private async void ParsePlayerBigPlaysAndTouchdownsForGame(int espnGameId, JObject playByPlayJsonObject, ArrayList playersInGame, ILogger log, IConfiguration configurationBuilder)
        {
            // get all plays in the current drive
            JToken playTokens = playByPlayJsonObject.SelectToken("drives.current.plays");

            // if the game started and there are plays in the current drive
            if (playTokens != null)
            {
                // go through every play in the current drive
                foreach (JToken playToken in playTokens)
                {
                    // Check if this play is a scoring play so we don't alert for both a long play and a touchdown.
                    // A scoring play can be a touchdown (offensive or defensive) and probably a field goal, safety,
                    // blocked punt, kick, etc, but need to verify these.
                    bool isScoringPlay = (bool)((JValue)playToken.SelectToken("scoringPlay")).Value;

                    if (isScoringPlay)
                    {
                        // we have the right play, so we need to see if the scoring type is a touchdown
                        string scoringType = ((JValue)playToken.SelectToken("scoringType.displayName")).Value.ToString();

                        if (scoringType.ToLower().Equals("touchdown"))
                        {
                            // the player name is displayed here, but it's usually first initial.lastname (G.Kittle), so we'd
                            // search for this player name in the players table for the current roster / matchup
                            string touchdownText = (string)((JValue)playToken.SelectToken("text")).Value;

                            // we now need to determine if this is a defensive or offensive touchdown
                            string touchdownType = ((JValue)playByPlayJsonObject.SelectToken("drives.current.displayResult")).Value.ToString();

                            // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                            // send a message to the service bus that the same touchdown was scored
                            int quarter = int.Parse(playToken.SelectToken("period.number").ToString());
                            string gameClock = (string)((JValue)playToken.SelectToken("clock.displayValue")).Value;

                            // displayResult = "Touchdown" is an offensive touchdown
                            if (touchdownType.ToLower().Equals("touchdown"))
                            {
                                // get yardage of the touchdown
                                int scoringPlayYardage = (int)Int64.Parse(((JValue)playToken.SelectToken("statYardage")).Value.ToString());

                                // check if any of players in the players list (current roster) have scored
                                foreach (PlayDetails playDetails in playersInGame)
                                {
                                    // get the player name as first <initial>.<lastname> to check if this is the player
                                    // who scored a touchdown
                                    string abbreviatedPlayerName = playDetails.PlayerName;
                                    int spaceIndex = abbreviatedPlayerName.IndexOf(' ');
                                    abbreviatedPlayerName = abbreviatedPlayerName[0] + "." + abbreviatedPlayerName.Substring(spaceIndex + 1);

                                    if (touchdownText.Contains(abbreviatedPlayerName) && (touchdownText.IndexOf("TOUCHDOWN") <= touchdownText.IndexOf(abbreviatedPlayerName)))
                                    {
                                        log.LogInformation("Did NOT add TD for " + playDetails.PlayerName + "; Player is a kicker.");
                                    }

                                    // We need to make sure that this player is the player who scored the TD and not the kicker kicking the XP. The
                                    // format of the text in the JSON Play By Play will be the following for a rushing and passing touchdown:
                                    // "text": "(5:30) (Shotgun) D.Samuel left end for 8 yards, TOUCHDOWN. R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                    //"text": "(8:55) (Shotgun) T.Brady pass short middle to M.Evans for 13 yards, TOUCHDOWN. R.Succop extra point is GOOD, Center-Z.Triner, Holder-J.Camarda."
                                    // It should be enough to ensure the occurence of the player has to be before the occurence of the text "TOUCHDOWN"
                                    else if (touchdownText.Contains(abbreviatedPlayerName) && (touchdownText.IndexOf("TOUCHDOWN") > touchdownText.IndexOf(abbreviatedPlayerName)))
                                    {
                                        // check if this was a passing touchdown and whether this player passed or received
                                        if (touchdownText.Contains("pass"))
                                        {
                                            // if this player threw the pass, their name will be to the left of the word "pass"
                                            if (touchdownText.IndexOf("pass") > touchdownText.IndexOf(abbreviatedPlayerName))
                                            {
                                                // find the name of the player this player threw a TD to, which will be in between the words "to" and the space after the players name
                                                int indexOfWordBeforeReceiverName = touchdownText.IndexOf("to");
                                                int indexOfSpaceAfterReceiverName = touchdownText.IndexOf(" ", indexOfWordBeforeReceiverName + ("to".Length + 2));
                                                string receiversName = touchdownText.Substring(indexOfWordBeforeReceiverName + ("to".Length + 1), indexOfSpaceAfterReceiverName - (indexOfWordBeforeReceiverName + ("to".Length + 1)));

                                                // The receiver's name will be first initial . last name (like T.Kelce), so let's look at the participants array
                                                // in the JSON and find the full name of the player
                                                string playerLastName = receiversName.Substring(receiversName.IndexOf(".") + 1);

                                                // no go through the list of participants of this play to find the full name of this player by just
                                                // matching the last name, which will hopefully be good enough.
                                                JToken participantTokens = playToken.SelectToken("participants");

                                                foreach (JToken participantToken in participantTokens)
                                                {
                                                    string receiverLastName = (string)((JValue)participantToken.SelectToken("athlete.lastName")).Value;

                                                    if (playerLastName.Equals(receiverLastName))
                                                    {
                                                        receiversName = (string)((JValue)participantToken.SelectToken("athlete.displayName")).Value;
                                                        break;
                                                    }
                                                }

                                                playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " threw a " + scoringPlayYardage + " yard TD to " + receiversName + "!";
                                            }
                                            else
                                            {
                                                playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " caught a " + scoringPlayYardage + " yard TD!";
                                            }
                                        }
                                        else
                                        {
                                            playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " ran for a " + scoringPlayYardage + " yard TD!";
                                        }

                                        // if this touchdown scored by this player was not already parsed, the touchdown will be added
                                        bool touchdownAdded = AddTouchdownDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                                        if (touchdownAdded)
                                        {
                                            log.LogInformation(playDetails.Message);
                                            
                                            await sendPlayMessage(playDetails, configurationBuilder);
                                        }
                                        else
                                        {
                                            log.LogInformation("Did NOT log TD for " + playDetails.PlayerName + "; TD already parsed earlier.");
                                        }
                                    }
                                }

                            }
                            // displayResult = "Interception Touchdown" is a pick six, so we need to figure out the defense
                            else if (touchdownType.ToLower().Equals("interception touchdown"))
                            {
                                // get the defensive player's name who intercepted the ball
                                // this will be "...INTERCEPTED by T.Hufanga at..." in the text node
                                int interceptedIndex = touchdownText.IndexOf("INTERCEPTED");
                                int textLengthOfInterceptedText = "INTERCEPTED by".Length;
                                int firstSpaceIndex = touchdownText.IndexOf(" ", interceptedIndex + textLengthOfInterceptedText);
                                int lastSpaceIndex = touchdownText.IndexOf(" ", firstSpaceIndex + 1);
                                string defensivePlayerShortName = touchdownText.Substring(firstSpaceIndex + 1, lastSpaceIndex - (firstSpaceIndex + 1));

                                // now look under "participants" and find this player and the team name abbreviation; from there, 
                                // we will see if the team name abbreviation (i.e. sf) is one of the defenses an owner has
                                JToken participantsTokens = playToken.SelectToken("participants");
                                foreach (JToken participantToken in participantsTokens)
                                {
                                    string athleteShortName = participantToken.SelectToken("athlete.shortName").ToString();

                                    // we need to remove any spaces here; for some reason, this value will have a space between first initial
                                    // and last name
                                    athleteShortName = athleteShortName.Replace(" ", "");

                                    if (athleteShortName.Equals(defensivePlayerShortName))
                                    {
                                        // let's find the team abbreviation so we can use this to check against the player name
                                        string teamAbbreviation = participantToken.SelectToken("athlete.team.abbreviation").ToString();

                                        // go through only the defenses in the list of players for each owner to see if there is a match
                                        foreach (PlayDetails playDetails in playersInGame)
                                        {
                                            if (playDetails.PlayerPosition.Equals("DEF") && (teamAbbreviation.ToLower().Equals(playDetails.TeamAbbreviation)))
                                            {
                                                // if this touchdown scored by this defense was not already parsed, the touchdown will be added
                                                bool touchdownAdded = AddTouchdownDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                                                if (touchdownAdded)
                                                {
                                                    playDetails.Message = "🎉 Defensive Touchdown! " + playDetails.PlayerName + " got a pick 6!";

                                                    log.LogInformation(playDetails.Message);
                                                    await sendPlayMessage(playDetails, configurationBuilder);
                                                }
                                                else
                                                {
                                                    log.LogInformation("Did NOT log TD for " + playDetails.PlayerName + "; TD already parsed earlier.");
                                                }

                                                // there is only one defense, so we can break out
                                                break;
                                            }
                                        }

                                        // at this point, we found the player who made the pick six, so we don't need to continue this loop of participants
                                        break;
                                    }
                                }
                            }
                            /*else if ("fumble touchdown???")
                            {

                            }
                            else if ("field goal")
                            {
                                // ignore field goals
                            }*/
                        }
                        //else if (scoringType.ToLower().Equals("safety"))
                        //{

                        //}
                        //else if (scoringType.ToLower().Equals("blocked punt / kick"))
                        //{

                        //}
                    }
                    // this isn't a scoring play, so check if it's a big play
                    else
                    {
                        // used to determine if a big play occured, whether it's passing, receiving, or rushing
                        bool bigPlayOccurred = false;

                        // we can get the yardage of the play from the statYardage property
                        int playYardage = (int)Int64.Parse(((JValue)playToken.SelectToken("statYardage")).Value.ToString());

                        // a passing or receiving play requires less yardage than a pass play to be considered a big play,
                        // so that is our minimum threshold for a big play; if we don't have that,w e can skip this play
                        // we can get the yardage of the play from the statYardage property
                        if (playYardage >= RECEIVING_AND_RUSHING_BIG_PLAY_YARDAGE)
                        {
                            // get the details of the touchdown
                            // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                            // send a message to the service bus that the same touchdown was scored
                            int quarter = int.Parse(playToken.SelectToken("period.number").ToString());
                            string gameClock = (string)((JValue)playToken.SelectToken("clock.displayValue")).Value;

                            // the player name is displayed here, but it's usually first initial.lastname (G.Kittle), so we'd
                            // search for this player name in the players table for the current roster / matchup
                            string bigPlayText = (string)((JValue)playToken.SelectToken("text")).Value;

                            foreach (PlayDetails playDetails in playersInGame)
                            {
                                // check if the player is involved in this play
                                // get the player name as first <initial>.<lastname> to check if this is the player
                                // who scored a touchdown
                                string abbreviatedPlayerName = playDetails.PlayerName;
                                int spaceIndex = abbreviatedPlayerName.IndexOf(' ');
                                abbreviatedPlayerName = abbreviatedPlayerName[0] + "." + abbreviatedPlayerName.Substring(spaceIndex + 1);

                                // if this player was involved in the play, let's determine the type of play
                                if (bigPlayText.Contains(abbreviatedPlayerName))
                                {
                                    string playType = ((JValue)playToken.SelectToken("type.abbreviation")).ToString();
                                    
                                    // If this is a pass play, we need to determine if this player threw the ball or received it
                                    if (playType.ToLower().Equals("rec"))
                                    {
                                        // If the occurence of the word "pass" occurs after the player name, then this player threw the pass;
                                        // otherwise, the player received it
                                        if (bigPlayText.IndexOf(abbreviatedPlayerName) < bigPlayText.IndexOf("pass"))
                                        {
                                            // find the name of the player this player threw a TD to, which will be in between the words "to" and the next space
                                            // the receiver name; so in the form of:
                                            // (6:31) (Shotgun) P.Mahomes pass deep right to M.Hardman pushed ob at LV 27 for 28 yards (J.Abram).
                                            int indexOfWordBeforeReceiverName = bigPlayText.IndexOf("to");
                                            int indexOfSpaceAfterReceiverName = bigPlayText.IndexOf(" ", indexOfWordBeforeReceiverName + ("to".Length + 2));
                                            string receiversName = bigPlayText.Substring(indexOfWordBeforeReceiverName + ("to".Length + 1), indexOfSpaceAfterReceiverName - (indexOfWordBeforeReceiverName + ("to".Length + 1)));

                                            // player threw a pass, so we'll only alert if it's above the passing yardage threshold
                                            if (playYardage >= PASSING_BIG_PLAY_YARDAGE)
                                            {
                                                bigPlayOccurred = true;
                                                playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " threw a pass of " + playYardage + " yards to " + receiversName + "!";
                                            }
                                        }
                                        else
                                        {
                                            bigPlayOccurred = true;

                                            // player received a pass, and we already know it's above the threshold since that was our
                                            // first check, so just send the alert
                                            playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " caught a pass of " + playYardage + " yards.";
                                        }
                                    }
                                    else if (playType.ToLower().Equals("rush"))
                                    {
                                        bigPlayOccurred = true;
                                        playDetails.Message = "🚀 Big play! " + playDetails.PlayerName + " rushed for " + playYardage + " yards.";
                                    }

                                    // if a big play occurred, let's add it to the database
                                    if (bigPlayOccurred)
                                    {
                                        // if this big play by this player was not already parsed, the big play will be added
                                        bool bigPlayAdded = AddBigPlayDetails(espnGameId, quarter, gameClock, playDetails.PlayerName, playDetails.Season, playDetails.OwnerId, playDetails.OpponentAbbreviation, playDetails.GameDate, log);

                                        if (bigPlayAdded)
                                        {
                                            log.LogInformation("Added big play for " + playDetails.PlayerName);
                                            await sendPlayMessage(playDetails, configurationBuilder);
                                        }
                                        else
                                        {
                                            log.LogInformation("Did NOT log big play for " + playDetails.PlayerName + "; big play already parsed earlier.");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        /// <summary>
        /// Send the play details as a message to the service bus' play queue.
        /// </summary>
        /// <param name="playDetails">The details of the particular play</param>
        /// <returns></returns>
        private async Task sendPlayMessage(PlayDetails playDetails, IConfiguration configurationBuilder)
        {
            // connection string to your Service Bus namespace
            string serviceBusSharedAccessSignature = configurationBuilder["ServiceBusSharedAccessKey"];
            string connectionString = "Endpoint=sb://fantasyfootballstattracker.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=" + serviceBusSharedAccessSignature;

            //string connectionString = "Endpoint=sb://fantasyfootballstattracker.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dZmufQp1JtwggtAqRFqxzUbOf5mloeA4LJUapntE+wY=";

            // name of your Service Bus queue
            string queueName = "touchdownqueue";

            // the client that owns the connection and can be used to create senders and receivers
            ServiceBusClient client;

            // the sender used to publish messages to the queue
            ServiceBusSender sender;

            // The Service Bus client types are safe to cache and use as a singleton for the lifetime
            // of the application, which is best practice when messages are being published or read
            // regularly.
            //
            // Create the clients that we'll use for sending and processing messages.
            client = new ServiceBusClient(connectionString);
            sender = client.CreateSender(queueName);

            try
            {
                ServiceBusMessage message = new ServiceBusMessage(JsonConvert.SerializeObject(playDetails));

                await sender.SendMessageAsync(message);
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }
                        
        /// <summary>
        /// Gets the SQL Access token so we can connect to the database
        /// </summary>
        /// <returns></returns>
        private static string GetAzureSqlAccessToken()
        {
            // See https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/services-support-managed-identities#azure-sql
            var tokenRequestContext = new TokenRequestContext(new[] { "https://database.windows.net//.default" });
            var tokenRequestResult = new DefaultAzureCredential().GetToken(tokenRequestContext);

            return tokenRequestResult.Token;
        }        
        
        /// <summary>
        /// Updates the TouchdownDetails table with a particular occurence of a touchdown. This touchdown has not already been
        /// parsed for this game.
        /// </summary>
        /// <param name="espnGameId">Live game ID</param>
        /// <param name="touchdownQuarter">The quarter this touchdown occurs</param>
        /// <param name="touchdownGameClock">The game clock when this touchdown occured</param>
        /// <param name="playerName">The player who scored the touchdown</param>
        /// <param name="log">The logger.</param>
        /// <returns></returns>
        private bool AddTouchdownDetails(int espnGameId, int touchdownQuarter, string touchdownGameClock, string playerName, int season, int ownerId, string opponentAbbreviation, DateTime gameDate, ILogger log)
        {
            bool touchdownAdded = false;

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandscheduledetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to add this touchdown for this player to the database if it hasn't already
                // been added
                using (SqlCommand command = new SqlCommand("AddTouchdownAlertDetails", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@EspnGameId", System.Data.SqlDbType.Int) { Value = espnGameId });
                    command.Parameters.Add(new SqlParameter("@TouchdownQuarter", System.Data.SqlDbType.Int) { Value = touchdownQuarter });
                    command.Parameters.Add(new SqlParameter("@TouchdownGameClock", System.Data.SqlDbType.NVarChar) { Value = touchdownGameClock });
                    command.Parameters.Add(new SqlParameter("@PlayerName", System.Data.SqlDbType.NVarChar) { Value = playerName });
                    command.Parameters.Add(new SqlParameter("@Season", System.Data.SqlDbType.Int) { Value = season });
                    command.Parameters.Add(new SqlParameter("@OpponentAbbreviation", System.Data.SqlDbType.NVarChar) { Value = opponentAbbreviation });
                    command.Parameters.Add(new SqlParameter("@GameDate", System.Data.SqlDbType.DateTime) { Value = gameDate });
                    command.Parameters.Add(new SqlParameter("@OwnerID", System.Data.SqlDbType.Int) { Value = ownerId });
                    command.Parameters.Add(new SqlParameter("@TouchdownTimeStamp", System.Data.SqlDbType.DateTime) { Value = currentEasterStandardTime });

                    touchdownAdded = (bool) command.ExecuteScalar();
                }

                sqlConnection.Close();
            }

            return touchdownAdded;
        }

        /// <summary>
        /// Updates the BigPlayDetails table with a particular occurence of a touchdown. This touchdown has not already been
        /// parsed for this game.
        /// </summary>
        /// <param name="espnGameId">Live game ID</param>
        /// <param name="quarter">The quarter this touchdown occurs</param>
        /// <param name="gameClock">The game clock when this touchdown occured</param>
        /// <param name="playerName">The player who scored the touchdown</param>
        /// <param name="log">The logger.</param>
        /// <returns></returns>
        private bool AddBigPlayDetails(int espnGameId, int quarter, string gameClock, string playerName, int season, int ownerId, string opponentAbbreviation, DateTime gameDate, ILogger log)
        {
            bool bigPlayAdded = false;

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                DataSource = "tcp:playersandscheduledetails.database.windows.net,1433",
                InitialCatalog = "PlayersAndSchedulesDetails",
                TrustServerCertificate = false,
                Encrypt = true
            };

            SqlConnection sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);

            try
            {
                string azureSqlToken = GetAzureSqlAccessToken();
                sqlConnection.AccessToken = azureSqlToken;
            }
            catch (Exception e)
            {
                log.LogInformation(e.Message);
            }

            // Get current EST time - If this is run on a machine with a differnet local time, DateTime.Now will not return the proper time
            TimeZoneInfo easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            DateTime currentEasterStandardTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, easternZone);

            using (sqlConnection)
            {
                sqlConnection.Open();

                // call stored procedure to add this touchdown for this player to the database if it hasn't already
                // been added
                using (SqlCommand command = new SqlCommand("AddBigPlayAlertDetails", sqlConnection))
                {
                    command.CommandType = System.Data.CommandType.StoredProcedure;
                    command.Parameters.Add(new SqlParameter("@EspnGameId", System.Data.SqlDbType.Int) { Value = espnGameId });
                    command.Parameters.Add(new SqlParameter("@BigPlayQuarter", System.Data.SqlDbType.Int) { Value = quarter });
                    command.Parameters.Add(new SqlParameter("@BigPlayGameClock", System.Data.SqlDbType.NVarChar) { Value = gameClock });
                    command.Parameters.Add(new SqlParameter("@PlayerName", System.Data.SqlDbType.NVarChar) { Value = playerName });
                    command.Parameters.Add(new SqlParameter("@Season", System.Data.SqlDbType.Int) { Value = season });
                    command.Parameters.Add(new SqlParameter("@OpponentAbbreviation", System.Data.SqlDbType.NVarChar) { Value = opponentAbbreviation });
                    command.Parameters.Add(new SqlParameter("@GameDate", System.Data.SqlDbType.DateTime) { Value = gameDate });
                    command.Parameters.Add(new SqlParameter("@OwnerID", System.Data.SqlDbType.Int) { Value = ownerId });
                    command.Parameters.Add(new SqlParameter("@BigPlayTimeStamp", System.Data.SqlDbType.DateTime) { Value = currentEasterStandardTime });

                    bigPlayAdded = (bool)command.ExecuteScalar();
                }

                sqlConnection.Close();
            }

            return bigPlayAdded;
        }
    }
}