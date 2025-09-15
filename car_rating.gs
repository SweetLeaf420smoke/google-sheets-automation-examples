function calculateCarScores() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  const data = sheet.getDataRange().getValues();

  const weights = {
    price: 500,
    mileage: 1000,
    age: 250,
  };

  const brandCoef = {
    "Toyota": 1.3,
    "Honda": 1.25,
    "Nissan": 1.2,
    "Volkswagen": 1.1,
    "Volvo": 1.1,
    "Mitsubishi": 1.0,
    "Jeep": 0.9,
  };

  const currentYear = new Date().getFullYear();
  sheet.getRange(1, 3).setValue("Score"); // колонка C

  for (let i = 1; i < data.length; i++) {
    let priceStr = data[i][5];   // F: Цена
    let year = data[i][6];       // G: Год
    let mileageStr = data[i][7]; // H: Пробег
    let model = data[i][3] || ""; // D: Марка

    if (!priceStr && !year && !mileageStr) continue;

    let price = priceStr ? parseInt(String(priceStr).replace(/[^\d]/g, ""), 10) : 0;
    let mileage = mileageStr ? parseInt(String(mileageStr).replace(/[^\d]/g, ""), 10) : 0;
    let age = (year && !isNaN(year)) ? currentYear - year : 0;

    let score = 0;

    if (price > 0) score += weights.price / Math.sqrt(price);
    if (mileage > 0) score += weights.mileage / Math.sqrt(mileage);

    if (age > 0) {
      if (age <= 15) {
        score += weights.age / 100;
      } else if (age <= 20) {
        score += weights.age / (age * 1.5);
      } else if (age <= 25) {
        score += weights.age / (age * 2);
      } else {
        score += weights.age / (age * 4);
      }
    }

    let brand = Object.keys(brandCoef).find(b => model.includes(b)) || "";
    let coef = brandCoef[brand] || 1.0;

    score *= coef;

    sheet.getRange(i + 1, 3).setValue(score.toFixed(2));
  }
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("🚗 Cars")
    .addItem("Calculate Scores", "calculateCarScores")
    .addToUi();
}
