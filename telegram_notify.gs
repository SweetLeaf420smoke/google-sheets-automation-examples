/**
 * Send one message to Telegram
 */
function sendMessageToTelegram(chatId, message, token) {
  var url = "https://api.telegram.org/bot" + token + "/sendMessage";
  var payload = {
    "chat_id": chatId,
    "text": message
  };
  
  var options = {
    "method": "post",
    "muteHttpExceptions": false,
    "payload": payload
  };
  
  var response = UrlFetchApp.fetch(url, options);
  Logger.log('Telegram response: ' + response.getContentText());
}

/**
 * Scheduled broadcast from Google Sheet
 * Columns: A = Message, B = Status ("НЕТ"/"ДА"), C = Date
 */
function sendScheduledMessagesFromSheet() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Sheet1');
  var data = sheet.getDataRange().getValues();  

  var token = "YOUR_BOT_TOKEN"; 
  var chatId = "YOUR_CHAT_ID";

  var today = new Date();
  today.setHours(0, 0, 0, 0);

  for (var i = 0; i < data.length; i++) {
    var messageDate = new Date(data[i][2]); // column C
    messageDate.setHours(0, 0, 0, 0);

    var messageText = data[i][0]; // column A
    var status = data[i][1]; // column B

    if (messageDate.getTime() === today.getTime() && status !== "ДА") {
      sendMessageToTelegram(chatId, messageText, token);
      sheet.getRange(i + 1, 2).setValue("ДА");
    }
  }
}
