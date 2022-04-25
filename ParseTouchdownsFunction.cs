namespace TouchdownAlertFunction
{
    using Azure.Messaging.ServiceBus;
    using HtmlAgilityPack;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Extensions.Http;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Text.Json;
    using System.Threading.Tasks;

    public class TouchdownAlert
    {
        private HtmlDocument _playByPlayDoc = new HtmlDocument();
        private JObject _playByPlayJsonObject;

        public TouchdownAlert()
        {
            //_playByPlayDoc.Load("C:\\fantasy Football\\playbyplay.json");
            //playByPlayJsonObject = GetPlayByPlayJsonObject();
            //_playByPlayJsonObject = JObject.Parse(_playByPlayDoc.Text);
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
        //public void RunSunday([TimerTrigger("*/10 * 11 * 4 5")] TimerInfo myTimer, ILogger log)
        public void RunSunday([TimerTrigger("*/10 * 13-23 * 9-1 0")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Sunday games at " + DateTime.Now);

            // TODO: The following things need to happen, in this order:
            // 1 - Check the redis cache for data (data should expire after monday night games)
            //   1a - if data isn't expired, store data into hashtables like the parser in the main app, and use this to check players against TDs parsed
            //   1b - if data is expired, query currentroster table to get roster and store relevant information in cache and have it expire after monday's game

            
            //parseTouchdowns();

            sendTouchdownMessage();
            //string name = req.Query["name"];

            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;

            //string responseMessage = string.IsNullOrEmpty(name)
            //    ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
            //    : $"Hello, {name}. This HTTP triggered function executed successfully.";

            //return Task.FromResult<IActionResult>(new OkResult());
        }

        // The timer trigger should run every 10 seconds on Thursdays from 8-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} 20 is at 20 minutes past the hour (thursday night gmaes start at 8:20)
        // {hour} 20-23 is 8pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-12 is Sept-Dec
        // {day of week} 4 is Thursday
        [FunctionName("ParseTouchdownsThursday")]
        public void RunThursday([TimerTrigger("*/10 20 20-23 * 9-12 4")] TimerInfo myTimer, ILogger log)
        //public void RunThursday([TimerTrigger("*/10 12-15 12-13 * 4 5")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Thursday games at " + DateTime.Now);

            //parseTouchdowns();

            sendTouchdownMessage();
            //string name = req.Query["name"];

            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;

            //string responseMessage = string.IsNullOrEmpty(name)
            //    ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
            //    : $"Hello, {name}. This HTTP triggered function executed successfully.";

            //return Task.FromResult<IActionResult>(new OkResult());
        }

        // The timer trigger should run every 10 seconds on Mondays from 8pm-11:59pm, Sept-Jan
        // * * * * * *
        // {second} {minute} {hour} {day of month} {month} {day of week}
        // for the trigger - */10 * 13 * 9-1 0 - each component is:
        // {second} */10 is every 10 seconds
        // {minute} 15 is at every 15 minutes past the hours (monday night gmaes start at 8:15)
        // {hour} 20-23 is 8pm-11:59pm
        // {day of the month} * is every day
        // {month} 9-1 is Sept-Jan
        // {day of week} 1 is Monday
        [FunctionName("ParseTouchdownsMonday")]
        public void RunMonday([TimerTrigger("*/10 15 20-23 * 9-1 1")] TimerInfo myTimer, ILogger log)
        //public void RunMonday([TimerTrigger("*/30 * 11 * 4 5")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request for Monday games at " + DateTime.Now);

            //parseTouchdowns();

            sendTouchdownMessage();
            //string name = req.Query["name"];

            //string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            //dynamic data = JsonConvert.DeserializeObject(requestBody);
            //name = name ?? data?.name;

            //string responseMessage = string.IsNullOrEmpty(name)
            //    ? "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response."
            //    : $"Hello, {name}. This HTTP triggered function executed successfully.";

            //return Task.FromResult<IActionResult>(new OkResult());
        }

        /// <summary>
        /// When a game is in progress, the play by play data is updated in the javascript function's espn.gamepackage.data variable.
        /// This method will find this variable and pull out the JSON and store it so we can parse the live play by play data.
        /// </summary>
        /// <returns>The JSON object representing the play by play data.</returns>
        public JObject GetPlayByPlayJsonObject()
        {
            JObject playByPlayJsonObject = null;

            var playByPlayJavaScriptNode = _playByPlayDoc.DocumentNode.SelectNodes("//script[@type='text/javascript']");

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

                            break;
                        }
                    }

                    break;
                }
            }

            return playByPlayJsonObject;
        }

        public void parseTouchdowns()
        {
            // each play token is a drive, so we will go through this to parse all player stats
            JToken driveTokens = _playByPlayJsonObject.SelectToken("drives.previous");

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
                            string quarter = playToken.SelectToken("period.number").ToString();
                            string gameClock = (string)((JValue)playToken.SelectToken("clock.displayValue")).Value;

                            // the player name is displayed here, but it's usually first initial.lastname (G.Kittle), so we'd
                            // search for this player name in the players table for the current roster / matchup
                            string touchdownText = (string)((JValue)playToken.SelectToken("text")).Value;
                        }
                    }
                }
            }
        }

        private async Task sendTouchdownMessage()
        {
            // connection string to your Service Bus namespace
            string connectionString = "Endpoint=sb://fantasyfootballstattracker.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=dZmufQp1JtwggtAqRFqxzUbOf5mloeA4LJUapntE+wY=";

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

            // create a batch 
            using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

            for (int i = 1; i <= 3; i++)
            {
                TouchdownDetails touchdown = new TouchdownDetails();
                touchdown.PlayerName = $"Player X";
                touchdown.PhoneNumber = "703-463-6826";

                string jsonTouchdown = JsonSerializer.Serialize(touchdown);

                // try adding a message to the batch
                //if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Message {i}")))
                if (!messageBatch.TryAddMessage(new ServiceBusMessage(jsonTouchdown)))
                {
                    // if it is too large for the batch
                    //throw new Exception($"The message {i} is too large to fit in the batch.");
                }
            }

            try
            {
                // Use the producer client to send the batch of messages to the Service Bus queue
                await sender.SendMessagesAsync(messageBatch);
                //Console.WriteLine($"A batch of 3 messages has been published to the queue.");
            }
            finally
            {
                // Calling DisposeAsync on client types is required to ensure that network
                // resources and other unmanaged objects are properly cleaned up.
                await sender.DisposeAsync();
                await client.DisposeAsync();
            }
        }
    }
}