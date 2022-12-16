namespace PlayAlertFunction
{
    using Azure.Core;
    using Azure.Identity;
    using Azure.Messaging.ServiceBus;
    using Azure.Storage;
    using Azure.Storage.Blobs;
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
        public void RunSunday([TimerTrigger("*/10 * 13-23 * 9-12 0")] TimerInfo myTimer, ILogger log, ExecutionContext context)
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

        // The timer trigger should run every 10 seconds on Saturdays from 1-11:59pm, Dec-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} 20 is at 20 minutes past the hour (thursday night games start at 8:20) - using *
        // {hour} 20-23 is 8pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-12 is Sept-Dec
        // {day of week} 6 is Saturday
        [FunctionName("ParseTouchdownsSaturday")]
        public void RunSaturday([TimerTrigger("*/10 * 13-23 * 12 6")] TimerInfo myTimer, ILogger log, ExecutionContext context)
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

                    //ParsePlayerBigPlaysAndTouchdownsForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, log, configurationBuilder);
                    ParsePlayerTouchdownsForGame(int.Parse((string)key), playByPlayJsonObject, playersInGame, log, configurationBuilder);
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

            var playByPlayJavaScriptNodes = playByPlayDoc.DocumentNode.SelectNodes("//script[@type='text/javascript']");

            foreach (var scriptNode in playByPlayJavaScriptNodes)
            {
                // the script will have:
                // window['__espnfitt__'] = { "app": {.... <all json> }
                if (scriptNode.InnerText.Contains("window['__espnfitt__']"))
                {
                    string content = scriptNode.InnerText.Trim();
                    int equalIndex = content.IndexOf("=");

                    // there is a trailing ;, so pull that off
                    string jsonContent = content.Substring(equalIndex + 1, content.Length - (equalIndex + 2));

                    playByPlayJsonObject = JObject.Parse(jsonContent);

                    break;
                }
            }

            return playByPlayJsonObject;
        }

        /// <summary>
        /// With the new update to the JSON doc, parsing touchdowns in a drive is not as easy as each play no longer has the
        /// necessary details and the text description is fairly complex. There is now a scrPlayGrps node which has only
        /// scoring plays, which we can check for TDs.
        /// </summary>
        /// <param name="espnGameId"></param>
        /// <param name="playByPlayJsonObject"></param>
        /// <param name="playersInGame"></param>
        private async void ParsePlayerTouchdownsForGame(int espnGameId, JObject playByPlayJsonObject, ArrayList playersInGame, ILogger log, IConfiguration configurationBuilder)
        {
            // flag determining whether or not a touchdown was processed for a player so we know if we should add this
            // to the database and send the message to the service hub so the logic app will process it and send a text
            // message to the owner
            bool touchdownProcessed = false;

            // TODO: Need to check to see how defensive TDs are displayed here
            JToken scoringPlaysArray = (JArray)playByPlayJsonObject.SelectToken("page.content.gamepackage.scrSumm.scrPlayGrps");

            foreach (JToken scoringPlayTokens in scoringPlaysArray)
            {
                // if there are no scoring plays (touchdowns, FGs, etc), this section may be null
                if (scoringPlayTokens != null)
                {
                    // go through each scoring play and check for a touchdown
                    foreach (JToken scoringPlayToken in scoringPlayTokens)
                    {
                        // the typeAbbreviation attribute will be "TD" for a touchdown
                        string scoringType = ((JValue)scoringPlayToken.SelectToken("typeAbbreviation")).Value.ToString();

                        if (scoringType.Equals("TD"))
                        {
                            string touchdownText = ((JValue)scoringPlayToken.SelectToken("text")).Value.ToString();

                            // we will cache the quarter and game clock so the next time we check the live JSON data, we don't
                            // send a message to the service bus that the same touchdown was scored
                            int quarter = int.Parse(scoringPlayToken.SelectToken("periodNum").ToString());
                            string gameClock = (string)((JValue)scoringPlayToken.SelectToken("clock")).Value;

                            // check if any of players in the players list (current roster) have scored
                            foreach (PlayDetails playDetails in playersInGame)
                            {
                                // It appears that the a player who rushed or received a TD will have their name appear as the first part of the text
                                // and the QB will appear after the "from" text such as:
                                // Rush:
                                //   "Christian McCaffrey 1 Yd Rush, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                //   "Austin Ekeler 1 Yd Run (Cameron Dicker Kick)" or
                                //   
                                // Pass (it looks likt he first one here is what is shown during live games; when games end, it changes to the 2nd, so we should only really
                                // care about the first one)
                                //   "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                //   "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)" (this will work for both WR/RB and QB) or
                                //   
                                // Fumble Recovery:
                                //   "Tyreek Hill 57 Yd Fumble Recovery (Jason Sanders Kick)" for an offensive fumble recovery for a TD
                                // let's check for a player who rushed or received a TD or picked up an offensive fumble and ran it in for a TD
                                if (touchdownText.StartsWith(playDetails.PlayerName))
                                {
                                    // regardless of the play, we need to get the yardage
                                    int touchdownPlayYardage = GetTouchdownPlayYardage(touchdownText);

                                    string passingPlayer = "";

                                    // if this is a pass, the word "pass" will be in the text and we need to pull out the name of the player
                                    // who threw the TD
                                    if (touchdownText.ToLower().Contains("pass"))
                                    {
                                        touchdownProcessed = true;

                                        passingPlayer = GetPassingPlayerName(touchdownText);

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " caught a " + touchdownPlayYardage + " yard TD from " + passingPlayer + "!";
                                    }
                                    // otherwise if it's a fumble recovery for a TD
                                    else if (touchdownText.ToLower().Contains("fumble recovery"))
                                    {
                                        touchdownProcessed = true;

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " recovered a fumble for a " + touchdownPlayYardage + " yard TD!";
                                    }
                                    // otherwise, i'ts a rushing TD
                                    else if (touchdownText.ToLower().Contains("run") || touchdownText.ToLower().Contains("rush"))
                                    {
                                        touchdownProcessed = true;

                                        playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " ran for a " + touchdownPlayYardage + " yard TD!";
                                    }
                                    else
                                    {
                                        log.LogInformation("Unknown! Play text: " + touchdownText);
                                    }
                                }
                                // otherwise, if this player name is in the text, then they threw a TD pass, such as this one from Brock Purdy
                                // "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
                                // This next one is only in this format with the parens for the kicker after the game ends
                                // "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)"
                                else if (touchdownText.Contains(playDetails.PlayerName))
                                {
                                    touchdownProcessed = true;

                                    string passingPlayer = GetPassingPlayerName(touchdownText);

                                    // get the name of the player this player threw a TD to
                                    string[] wordsInTouchdownText = touchdownText.Split(" ");

                                    // get the integer in this string, which will be the yardage of the play
                                    int touchdownPlayYardage = GetTouchdownPlayYardage(touchdownText);

                                    // now that we have the yardage, we can grab the players name to the left of this, which is the name of the
                                    // player this QB threw a touchdown to
                                    string receivingPlayer = touchdownText.Substring(0, touchdownText.IndexOf(touchdownPlayYardage.ToString()) - 1);

                                    playDetails.Message = "🎉 Touchdown! " + playDetails.PlayerName + " threw a " + touchdownPlayYardage + " yard TD to " + receivingPlayer + "!";
                                }

                                // if a touchdown was processed, add the touchdown to the db and send the message to the service hub
                                if (touchdownProcessed)
                                {
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

                                    // reset the touchdown processed flag
                                    touchdownProcessed = false;
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Given a touchdown string from the touchdown play nodes, such as:
        /// "Austin Ekeler 1 Yd Run (Cameron Dicker Kick)" or
        /// "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)" (this will work for both WR/RB and QB) or
        /// "Tyreek Hill 57 Yd Fumble Recovery (Jason Sanders Kick)" for an offensive fumble recovery for a TD,
        /// find the first occurence (which is the only occurence) of an integer and return that.
        /// </summary>
        /// <param name="touchdownText"></param>
        /// <returns></returns>
        private int GetTouchdownPlayYardage(string touchdownText)
        {
            // regardless of the play, we need to get the yardage
            string[] wordsInTouchdownText = touchdownText.Split(" ");

            // get the integer in this string, which will be the yardage of the play
            int touchdownPlayYardage = -1;
            foreach (var word in wordsInTouchdownText)
            {
                // if the "word" is an integer, this will pass and we'll have our yardage
                if (int.TryParse(word, out int n))
                {
                    touchdownPlayYardage = int.Parse(word);
                    break;
                }
            }

            return touchdownPlayYardage;
        }

        /// <summary>
        /// When a player either receives or throws a touchdown, we need to get the name of the player who
        /// threw the touchdown. There are two formats for this:
        ///   "George Kittle Pass From Brock Purdy for 28 Yds, R.Gould extra point is GOOD, Center-T.Pepper, Holder-M.Wishnowsky."
        ///   "Tyreek Hill 60 Yd pass from Tua Tagovailoa (Jason Sanders Kick)" (this will work for both WR/RB and QB) or
        /// with the first one being what should be seen during a live game and the second one is what that first one is changed
        /// to once the game is over. So technically, since this is run only during live games, we should only care about the
        /// first format of the string.
        /// </summary>
        /// <param name="touchdownText">The text of the touchdown play.</param>
        /// <returns></returns>
        private string GetPassingPlayerName(string touchdownText)
        {
            string passingPlayer = "";

            // We're first checking if there is not a left paren, which should only be there for a completed game
            if (touchdownText.IndexOf("(") == -1)
            {
                // the name of the player who threw the TD is between the word "From" and the word "for"
                int indexOfWordFrom = touchdownText.IndexOf("From");
                int indexOfWordFor = touchdownText.IndexOf("for");

                passingPlayer = touchdownText.Substring(indexOfWordFrom + ("From".Length + 1), (indexOfWordFor - (indexOfWordFrom + "From".Length + 2)));
            }
            else if (touchdownText.IndexOf("(") != -1)
            {
                // the name of the player who threw the TD is between the word "from" and the first left paren
                int indexOfWordFrom = touchdownText.IndexOf("from");
                int indexOfFirstLeftParenthesis = touchdownText.IndexOf("(");

                passingPlayer = touchdownText.Substring(indexOfWordFrom + ("from".Length + 1), (indexOfFirstLeftParenthesis - (indexOfWordFrom + "from".Length + 2)));
            }

            return passingPlayer;
        }

        /// <summary>
        /// Parses the current drive of the JSON object for the given game to see if any of the players
        /// playing in this game just got a big play or a touchdown. If the touchdown has not yet been
        /// texted to the owner, based on game clock stored as last parsed touchdown in the database, a
        /// text will be sent to the owner.
        /// </summary>
        /// <param name="playByPlayJsonObject"></param>
        /// <param name="playersInGame"></param>
        /*private async void ParsePlayerBigPlaysAndTouchdownsForGame(int espnGameId, JObject playByPlayJsonObject, ArrayList playersInGame, ILogger log, IConfiguration configurationBuilder)
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
                            // updating to catch any other scoring play - there was a defense fumble recovery for a TD which wasn't alerted
                            else// if (touchdownType.ToLower().Contains("touchdown"))
                            {
                                // TESTING - we will export this json so we can see how the paylod looks for a fumble recovery for a touchdown or
                                // any other type of defensive score
                                await UploadJsonPlayByPlayDoc(espnGameId.ToString(), touchdownType, playByPlayJsonObject, configurationBuilder);
                            }
                            /*else if ("fumble touchdown???")
                            {

                            }
                            else if ("field goal")
                            {
                                // ignore field goals
                            }*/
        /*}
        else
        {
            // TESTING - we will export this json so we can see how the paylod looks for a safety, blocked punt / kick, etc
            await UploadJsonPlayByPlayDoc(espnGameId.ToString(), scoringType, playByPlayJsonObject, configurationBuilder);
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
}*/

        /// <summary>
        /// When an unknown play occurs, the live json play by play doc will be uploaded to Azure storage so it can be investigated and the code
        /// can be updated accordingly.
        /// </summary>
        /// <param name="espnGameId">The ESPN game id</param>
        /// <param name="touchdownType">The type of touchdown, which will help clarify what we are looking at in this JSON object</param>
        /// <param name="playByPlayJsonObject">The jSON object</param>
        /// <param name="configurationBuilder">Configuration builder to pull out the storage account key from settings</param>
        /// <returns></returns>
        private async Task UploadJsonPlayByPlayDoc(string espnGameId, string touchdownType, JObject playByPlayJsonObject, IConfiguration configurationBuilder)
        {
            string accountName = "playbyplayjsondocs";
            string accountKey = configurationBuilder["JsonDocStorageAccountKey"];
            StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
            string blobUri = "https://" + accountName + ".blob.core.windows.net";
            string blobName = espnGameId + touchdownType.ToLower();

            BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri(blobUri), sharedKeyCredential);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient("playbyplaydocs");

            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            await blobClient.UploadAsync(BinaryData.FromString(playByPlayJsonObject.ToString()), overwrite: true);
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