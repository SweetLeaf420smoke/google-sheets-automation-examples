
// ===== Ozon API Automation =====
// Все функции для работы с Ozon API в Google Sheets
// ⚠️ Заполни лист "Настройки" своими ключами:
// B2 = Client-Id, B3 = Api-Key, B4 = Дата от, B5 = Дата до
// B6 = Service Account, B7 = Client Performance, B8 = Secret Performance

===== buildOzonFullPivot =====
function buildOzonFullPivot() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Сводная Общая";
  const outSheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  outSheet.clear();

  // --- читаем рекламные данные ---
  const adsSheet = ss.getSheetByName("Озон Реклама Итог Все Данные");
  if (!adsSheet) throw new Error("Нет листа 'Озон Реклама Итог Все Данные'");
  const adsData = adsSheet.getDataRange().getValues();
  const adsHeaders = adsData.shift();

  // --- читаем аналитику ---
  const analSheet = ss.getSheetByName("Аналитика");
  if (!analSheet) throw new Error("Нет листа 'Аналитика'");
  const analData = analSheet.getDataRange().getValues();
  const analHeaders = analData.shift();

  // --- читаем транзакции ---
  const tranSheet = ss.getSheetByName("Транзакции");
  if (!tranSheet) throw new Error("Нет листа 'Транзакции'");
  const tranData = tranSheet.getDataRange().getValues();
  const tranHeaders = tranData.shift();

  // --- читаем остатки ---
  const stockSheet = ss.getSheetByName("Остатки");
  if (!stockSheet) throw new Error("Нет листа 'Остатки'");
  const stockData = stockSheet.getDataRange().getValues();
  const stockHeaders = stockData.shift();

  // =======================
  // индексы колонок
  // =======================
  const idxAdsSKU   = adsHeaders.indexOf("SKU");
  const idxAdsDate  = adsHeaders.indexOf("Дата");
  const idxAdsViews = adsHeaders.indexOf("Просмотры");
  const idxAdsClicks= adsHeaders.indexOf("Клики");
  const idxAdsOrders= adsHeaders.indexOf("Заказы");
  const idxAdsMoney = adsHeaders.indexOf("Сумма заказов ₽");
  const idxAdsSpent = adsHeaders.indexOf("Расход ₽");

  const idxAnalSKU  = analHeaders.indexOf("sku");
  const idxAnalDate = analHeaders.indexOf("day");
  const idxAnalRev  = analHeaders.indexOf("revenue");
  const idxAnalUnits= analHeaders.indexOf("ordered_units");

  const idxTranSKU  = tranHeaders.indexOf("sku");
  const idxTranDate = tranHeaders.indexOf("order_date");
  const idxTranAmt  = tranHeaders.indexOf("amount");
  const idxTranType = tranHeaders.indexOf("type");

  const idxStockSKU = stockHeaders.indexOf("sku");
  const idxStockName= stockHeaders.indexOf("name");
  const idxStockAvail = stockHeaders.indexOf("available_stock_count");

  // =======================
  // вспомогательные функции
  // =======================
  function normDate(d) {
    if (!d) return "";
    try {
      if (Object.prototype.toString.call(d) === "[object Date]") {
        return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
      } else {
        return Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), "yyyy-MM-dd");
      }
    } catch(e) {
      return d.toString().substring(0,10);
    }
  }

  const pivot = {};
  function key(sku, date) {
    return sku + "_" + date;
  }

  // =======================
  // реклама
  // =======================
  adsData.forEach(r => {
    const sku = r[idxAdsSKU];
    const date = normDate(r[idxAdsDate]);
    if (!sku || !date) return;
    const k = key(sku, date);
    if (!pivot[k]) pivot[k] = {sku, date};
    pivot[k].adsViews  = (pivot[k].adsViews || 0) + (r[idxAdsViews] || 0);
    pivot[k].adsClicks = (pivot[k].adsClicks|| 0) + (r[idxAdsClicks]|| 0);
    pivot[k].adsOrders = (pivot[k].adsOrders|| 0) + (r[idxAdsOrders]|| 0);
    pivot[k].adsMoney  = (pivot[k].adsMoney || 0) + (r[idxAdsMoney] || 0);
    pivot[k].adsSpent  = (pivot[k].adsSpent || 0) + (r[idxAdsSpent] || 0);
  });

  // =======================
  // аналитика
  // =======================
  analData.forEach(r => {
    const sku = r[idxAnalSKU];
    const date = normDate(r[idxAnalDate]);
    if (!sku || !date) return;
    const k = key(sku, date);
    if (!pivot[k]) pivot[k] = {sku, date};
    pivot[k].orgRevenue = (pivot[k].orgRevenue||0) + (r[idxAnalRev] || 0);
    pivot[k].orgUnits   = (pivot[k].orgUnits  ||0) + (r[idxAnalUnits] || 0);
  });

  // =======================
  // транзакции
  // =======================
  tranData.forEach(r => {
    const sku = r[idxTranSKU];
    const date = normDate(r[idxTranDate]);
    if (!sku || !date) return;
    const k = key(sku, date);
    if (!pivot[k]) pivot[k] = {sku, date};
    if (r[idxTranType] === "orders") {
      pivot[k].tranOrders = (pivot[k].tranOrders||0) + (r[idxTranAmt] || 0);
    } else if (r[idxTranType] === "returns") {
      pivot[k].tranReturns = (pivot[k].tranReturns||0) + (r[idxTranAmt] || 0);
    }
  });

  // =======================
  // остатки
  // =======================
  const stockBySku = {};
  const nameBySku  = {};
  stockData.forEach(r => {
    const sku = r[idxStockSKU];
    if (!sku) return;
    stockBySku[sku] = r[idxStockAvail] || 0;
    nameBySku[sku]  = r[idxStockName]  || "";
  });

  // =======================
  // формируем таблицу
  // =======================
  const headers = [
    "SKU","Название",
    "Дата",
    "Рекл.Показы","Рекл.Клики","Рекл.Заказы","Рекл.Сумма ₽","Рекл.Расход ₽",
    "Орг.Выручка ₽","Орг.Заказы",
    "Транз.Заказы ₽","Транз.Возвраты ₽",
    "Остаток"
  ];
  const result = [headers];

  Object.values(pivot).forEach(v => {
    result.push([
      v.sku,
      nameBySku[v.sku] || "",
      v.date,
      v.adsViews||0,
      v.adsClicks||0,
      v.adsOrders||0,
      v.adsMoney||0,
      v.adsSpent||0,
      v.orgRevenue||0,
      v.orgUnits||0,
      v.tranOrders||0,
      v.tranReturns||0,
      stockBySku[v.sku]||0
    ]);
  });

  // =======================
  // запись
  // =======================
  outSheet.getRange(1,1,result.length,result[0].length).setValues(result);
  Logger.log("✅ Сводная общая построена: " + (result.length-1) + " строк");
}

===== checkAllSheetsHeaders =====
function checkAllSheetsHeaders() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();

  sheets.forEach(sh => {
    const name = sh.getName();
    const values = sh.getDataRange().getValues();
    if (values.length === 0) {
      Logger.log("❌ " + name + " — пустой лист");
      return;
    }
    const headers = values[0];
    Logger.log("✅ " + name + " — " + JSON.stringify(headers));
  });
}

===== exportAllFunctionsCodeFast =====
function exportAllFunctionsCodeFast() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Ozon_Functions") || ss.insertSheet("Ozon_Functions");
  sheet.clear();

  const global = this;
  const functions = [];

  for (let key in global) {
    if (typeof global[key] === "function") {
      functions.push(key);
    }
  }

  functions.sort();

  const rows = [];
  functions.forEach(fnName => {
    try {
      const code = global[fnName].toString().split("\n");
      rows.push(["'===== " + fnName + " ====="]);   // заголовок
      code.forEach(line => rows.push(["'" + line])); // сам код
      rows.push([""]); // пустая строка
    } catch (e) {
      rows.push(["'===== " + fnName + " (код недоступен) ====="]);
      rows.push([""]);
    }
  });

  if (rows.length > 0) {
    sheet.getRange(1, 1, rows.length, 1).setValues(rows);
  }

  Logger.log("✅ Выгружено функций: " + functions.length);
}

===== exportOzonAdsCampaigns =====
function exportOzonAdsCampaigns() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Кампании";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const token = getPerformanceToken();
  const url = "https://api-performance.ozon.ru/api/client/campaign?page=1&pageSize=100";

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error("Ошибка HTTP " + resp.getResponseCode() + ": " + resp.getContentText());
  }

  const json = JSON.parse(resp.getContentText());
  const campaigns = json.list || [];
  Logger.log("📊 Получено кампаний: " + campaigns.length);

  if (!campaigns.length) return;

  const headers = [
    "ID", "Название", "Статус", "Тип", "Дата начала", "Дата конца",
    "Дневной бюджет", "Недельный бюджет", "Бюджет", "Оплата",
    "Создана", "Обновлена", "Автостратегия"
  ];
  sheet.appendRow(headers);

  const fmt = s => s ? Utilities.formatDate(new Date(s), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss") : "";

  const rows = campaigns.map(c => [
    c.id || "",
    c.title || "",
    c.state || "",
    c.advObjectType || "",
    c.fromDate || "",
    c.toDate || "",
    c.dailyBudget || "",
    c.weeklyBudget || "",
    c.budget || "",
    c.PaymentType || "",
    fmt(c.createdAt),
    fmt(c.updatedAt),
    c.productAutopilotStrategy || ""
  ]);

  sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  Logger.log("✅ Записано строк: " + rows.length);
}

===== exportOzonAdsDaily =====
function exportOzonAdsDaily() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = 'Озон Реклама Дневная';
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const fromStr = format(dateFrom);
  const toStr   = format(dateTo);

  const token = getPerformanceToken();

  const url = 'https://api-performance.ozon.ru/api/client/statistics/daily/json' +
              '?dateFrom=' + fromStr +
              '&dateTo=' + toStr;

  Logger.log('GET ' + url);

  const resp = UrlFetchApp.fetch(url, {
    method : 'get',
    headers: { 'Authorization': 'Bearer ' + token },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error('Ошибка HTTP ' + resp.getResponseCode() + ': ' + resp.getContentText());
  }

  const json = JSON.parse(resp.getContentText());
  const arr = json.content || json.data || json.rows || [];
  Logger.log('📊 Получено строк: ' + arr.length);

  if (!arr.length) return;

  sheet.appendRow([
    'Campaign ID', 'Title', 'Date',
    'Views', 'Clicks', 'Spent, ₽',
    'Avg Bid, ₽', 'Orders', 'Orders Money, ₽'
  ]);

  const rows = arr.map(r => [
    r.campaignId || r.id || '',
    r.title      || '',
    r.date       || '',
    r.views      || 0,
    r.clicks     || 0,
    r.moneySpent || 0,
    r.avgBid     || 0,
    r.orders     || 0,
    r.ordersMoney|| 0
  ]);

  sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  Logger.log(`✅ Записано строк: ${rows.length}`);
}

===== exportOzonAdsDailyExpense =====
function exportOzonAdsDailyExpense() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Итог";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // даты из листа Настройки
  const settings = ss.getSheetByName("Настройки");
  const dateFrom = Utilities.formatDate(new Date(settings.getRange("B4").getValue()), "GMT+3", "yyyy-MM-dd");
  const dateTo   = Utilities.formatDate(new Date(settings.getRange("B5").getValue()), "GMT+3", "yyyy-MM-dd");
  const token = getPerformanceToken();

  // --- 1. daily ---
  const urlDaily = "https://api-performance.ozon.ru/api/client/statistics/daily/json"
    + "?dateFrom=" + dateFrom + "&dateTo=" + dateTo;
  const respDaily = UrlFetchApp.fetch(urlDaily, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });
  const daily = JSON.parse(respDaily.getContentText()).rows || [];
  Logger.log("✅ daily загружено строк: %s", daily.length);

  // --- 2. expense ---
  const urlExpense = "https://api-performance.ozon.ru/api/client/statistics/expense/json"
    + "?dateFrom=" + dateFrom + "&dateTo=" + dateTo;
  const respExpense = UrlFetchApp.fetch(urlExpense, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });
  const expense = JSON.parse(respExpense.getContentText()).rows || [];
  Logger.log("✅ expense загружено строк: %s", expense.length);

  const expenseMap = {};
  expense.forEach(r => {
    const key = r.id + "_" + r.date;
    expenseMap[key] = r;
  });

  // --- 3. Campaign Stats (для CTR) ---
  const urlGen = "https://api-performance.ozon.ru/api/client/statistics/json";
  const campaignIds = [...new Set(daily.map(r => r.id))];
  const payload = { campaigns: campaignIds, dateFrom, dateTo, groupBy: "DATE" };
  const respGen = UrlFetchApp.fetch(urlGen, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });
  const uuid = JSON.parse(respGen.getContentText()).UUID;
  if (!uuid) throw new Error("Не удалось сгенерировать отчёт CTR");

  let state = "NOT_STARTED";
  for (let i = 0; i < 30; i++) {
    const statusUrl = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
    const respStatus = UrlFetchApp.fetch(statusUrl, {
      method: "get",
      headers: { Authorization: "Bearer " + token }
    });
    const status = JSON.parse(respStatus.getContentText());
    state = status.state || "UNKNOWN";
    if (state === "OK") break;
    if (state === "ERROR") throw new Error("Ошибка генерации CTR отчёта");
    Utilities.sleep(3000);
  }

  if (state !== "OK") throw new Error("CTR отчёт не готов");

  const urlReport = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const respReport = UrlFetchApp.fetch(urlReport, {
    method: "get",
    headers: { Authorization: "Bearer " + token }
  });
  const ctrJson = JSON.parse(respReport.getContentText());

  const ctrMap = {};
  Object.keys(ctrJson).forEach(cid => {
    const rows = (ctrJson[cid].report && ctrJson[cid].report.rows) || [];
    rows.forEach(r => {
      let d = r.date;
      if (d.includes(".")) {
        const parts = d.split(".");
        d = `${parts[2]}-${parts[1].padStart(2,"0")}-${parts[0].padStart(2,"0")}`;
      }
      const key = cid + "_" + d;
      ctrMap[key] = parseFloat(r.ctr) || 0;
    });
  });

  // --- 4. Итоговая таблица ---
  const headers = [
    "Campaign ID","Title","Date",
    "Views","Clicks","CTR %","Avg Bid ₽",
    "Orders","Orders Money ₽",
    "Spent ₽","Bonus ₽","Prepayment ₽"
  ];
  sheet.appendRow(headers);

  const rows = daily.map(r => {
    const key = r.id + "_" + r.date;
    const e = expenseMap[key] || {};
    const ctrVal = ctrMap[key] || (r.views > 0 ? (r.clicks / r.views * 100) : 0);

    return [
      r.id || "",
      r.title || "",
      r.date || "",
      r.views || 0,
      r.clicks || 0,
      ctrVal,
      r.avgBid || 0,
      r.orders || 0,
      r.ordersMoney || 0,
      e.moneySpent || 0,
      e.bonusSpent || 0,
      e.prepaymentSpent || 0
    ];
  });

  sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
  Logger.log("✅ Записано строк: " + rows.length);
}

===== exportOzonAdsExpense =====
function exportOzonAdsExpense() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Расходы";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const fromStr = format(dateFrom);
  const toStr   = format(dateTo);

  const token = getPerformanceToken();

  const url = "https://api-performance.ozon.ru/api/client/statistics/expense/json" +
              "?dateFrom=" + fromStr +
              "&dateTo=" + toStr;

  Logger.log("GET " + url);

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error("Ошибка HTTP " + resp.getResponseCode() + ": " + resp.getContentText());
  }

  const json = JSON.parse(resp.getContentText());
  const arr = json.rows || [];
  Logger.log("📊 Получено строк: " + arr.length);

  if (!arr.length) return;

  sheet.appendRow(["Campaign ID", "Title", "Date", "Spent ₽", "Bonus ₽", "Prepayment ₽"]);

  const rows = arr.map(r => [
    r.id || "",
    r.title || "",
    r.date || "",
    r.moneySpent || "0",
    r.bonusSpent || "0",
    r.prepaymentSpent || "0"
  ]);

  sheet.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
  Logger.log(`✅ Записано строк: ${rows.length}`);
}

===== exportOzonAdsFullReport =====
function exportOzonAdsFullReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Итог Все Данные";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const dailySheet   = ss.getSheetByName("Озон Реклама Итог");
  const productsSheet= ss.getSheetByName("Озон Реклама Продукты");
  const ordersSheet  = ss.getSheetByName("Озон Реклама Заказы");

  if (!dailySheet || !productsSheet || !ordersSheet) {
    throw new Error("Нужны листы 'Озон Реклама Итог', 'Озон Реклама Продукты' и 'Озон Реклама Заказы'");
  }

  const dailyData = dailySheet.getDataRange().getValues();
  const dailyHeaders = dailyData.shift();

  const prodData = productsSheet.getDataRange().getValues();
  const prodHeaders = prodData.shift();
  const idxProdSKU = prodHeaders.indexOf("SKU");
  const idxProdTitle = prodHeaders.indexOf("Title");
  const idxProdPrice = prodHeaders.indexOf("Price");
  const prodBySKU = {};
  prodData.forEach(r => {
    const sku = r[idxProdSKU];
    if (sku) prodBySKU[sku] = r;
  });

  const ordersData = ordersSheet.getDataRange().getValues();
  const ordersHeaders = ordersData.shift();
  const idxOrdSKU = ordersHeaders.indexOf("sku");
  const idxOrdDate = ordersHeaders.indexOf("date");
  const idxOrdQty = ordersHeaders.indexOf("quantity");
  const idxOrdPrice = ordersHeaders.indexOf("price");
  const idxOrdSalePrice = ordersHeaders.indexOf("salePrice");

  const ordersByDate = {};
  ordersData.forEach(r => {
    const dateRaw = r[idxOrdDate];
    const sku = r[idxOrdSKU];
    if (!dateRaw || !sku) return;

    const date = parseOrderDate(dateRaw);
    if (!ordersByDate[date]) ordersByDate[date] = [];
    ordersByDate[date].push(r);
  });

  const headers = [
    "ID кампании","Название кампании","SKU","Дата",
    "Просмотры","Клики","CTR %","Расход ₽","Средняя ставка ₽",
    "Заказы","Сумма заказов ₽",
    "Кол-во (из заказов)","Сумма по продажам ₽",
    "Название товара","Цена ₽"
  ];
  const result = [headers];

  dailyData.forEach(r => {
    const campaignId = r[0];
    const title = r[1];
    const date = Utilities.formatDate(new Date(r[2]), "GMT+2", "yyyy-MM-dd");
    const views = r[3];
    const clicks = r[4];
    const ctr = r[5];
    const avgBid = r[6];
    const orders = r[7];
    const ordersMoney = r[8];
    const spent = r[9];

    const dayOrders = ordersByDate[date] || [];

    if (dayOrders.length > 0) {
      dayOrders.forEach(ord => {
        const sku = ord[idxOrdSKU] || "";
        const qty = Number(ord[idxOrdQty]) || 0;
        let salePrice = parseNumber(ord[idxOrdSalePrice]);
        if (!salePrice) salePrice = parseNumber(ord[idxOrdPrice]);
        const salesSum = qty * salePrice;

        let prodTitle = "";
        let price = "";
        if (prodBySKU[sku]) {
          prodTitle = prodBySKU[sku][idxProdTitle] || "";
          price = prodBySKU[sku][idxProdPrice] || "";
        }
        result.push([
          campaignId, title, sku, date,
          views, clicks, ctr, spent, avgBid,
          orders, ordersMoney,
          qty, salesSum,
          prodTitle, price
        ]);
      });
    } else {
      result.push([
        campaignId, title, "", date,
        views, clicks, ctr, spent, avgBid,
        orders, ordersMoney,
        "", "",
        "", ""
      ]);
    }
  });

  sheet.getRange(1,1,result.length,result[0].length).setValues(result);
  Logger.log("✅ Итоговая таблица собрана: " + (result.length-1) + " строк");
}

===== exportOzonAdsOrdersReport =====
function exportOzonAdsOrdersReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Заказы";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // даты из листа "Настройки"
  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const dateFrom = new Date(settings.getRange("B4").getValue());
  const dateTo   = new Date(settings.getRange("B5").getValue());

  const token = getPerformanceToken();

  // шаг 1 — генерируем отчёт
  const urlGenerate = "https://api-performance.ozon.ru/api/client/statistic/orders/generate/json";
  const payload = { from: dateFrom.toISOString(), to: dateTo.toISOString() };
  const respGenerate = UrlFetchApp.fetch(urlGenerate, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const genJson = JSON.parse(respGenerate.getContentText());
  if (!genJson.UUID) throw new Error("Ошибка генерации: " + respGenerate.getContentText());
  const uuid = genJson.UUID;
  Logger.log("🟢 UUID отчёта: " + uuid);

  // шаг 2 — ждём готовности
  let state = "Pending";
  for (let i = 0; i < 30; i++) { // максимум 30 попыток
    const urlStatus = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
    const respStatus = UrlFetchApp.fetch(urlStatus, {
      method: "get",
      headers: { Authorization: "Bearer " + token },
      muteHttpExceptions: true
    });
    const statusJson = JSON.parse(respStatus.getContentText());
    state = statusJson.state || "UNKNOWN";
    Logger.log("⏳ Попытка " + (i+1) + ": " + state);

    if (state === "OK") break;
    if (state === "ERROR") throw new Error("❌ Ошибка генерации отчёта");
    Utilities.sleep(5000); // подождать 5 сек
  }

  if (state !== "OK") throw new Error("Отчёт не готов, попробуйте позже");

  // шаг 3 — скачиваем результат
  const urlReport = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const respReport = UrlFetchApp.fetch(urlReport, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  const reportText = respReport.getContentText();
  let rows;
  try {
    const json = JSON.parse(reportText);
    if (!json.rows || !json.rows.length) throw new Error("Нет данных");
    rows = json.rows;
  } catch(e) {
    throw new Error("Не удалось распарсить JSON отчёта: " + e);
  }

  // шаг 4 — пишем в лист
  const headers = Object.keys(rows[0]);
  const data = [headers];
  rows.forEach(r => data.push(headers.map(h => r[h] || "")));

  sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
  Logger.log("✅ Загружено строк: " + rows.length);
}

===== exportOzonAdsProductsReport =====
function exportOzonAdsProductsReport() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Продукты";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // даты из листа "Настройки"
  const settings = ss.getSheetByName("Настройки");
  const dateFrom = new Date(settings.getRange("B4").getValue());
  const dateTo   = new Date(settings.getRange("B5").getValue());
  const token = getPerformanceToken();

  // 1. Генерируем отчёт
  const urlGenerate = "https://api-performance.ozon.ru/api/client/statistic/products/generate/json";
  const payload = { from: dateFrom.toISOString(), to: dateTo.toISOString() };
  const respGenerate = UrlFetchApp.fetch(urlGenerate, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const genJson = JSON.parse(respGenerate.getContentText());
  if (!genJson.UUID) throw new Error("Ошибка генерации: " + respGenerate.getContentText());
  const uuid = genJson.UUID;
  Logger.log("🟢 UUID отчёта: " + uuid);

  // 2. Ждём готовности
  let state = "Pending";
  for (let i = 0; i < 30; i++) {
    const urlStatus = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
    const respStatus = UrlFetchApp.fetch(urlStatus, {
      method: "get",
      headers: { Authorization: "Bearer " + token },
      muteHttpExceptions: true
    });
    const statusJson = JSON.parse(respStatus.getContentText());
    state = statusJson.state || "UNKNOWN";
    Logger.log("⏳ Попытка " + (i+1) + ": " + state);

    if (state === "OK") break;
    if (state === "ERROR") throw new Error("❌ Ошибка генерации отчёта");
    Utilities.sleep(5000);
  }

  if (state !== "OK") throw new Error("Отчёт не готов, попробуйте позже");

  // 3. Скачиваем результат
  const urlReport = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const respReport = UrlFetchApp.fetch(urlReport, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  const reportText = respReport.getContentText();
  const json = JSON.parse(reportText);
  if (!json.rows || !json.rows.length) throw new Error("Нет данных");

  // 4. Пишем в лист
  const headers = Object.keys(json.rows[0]);
  const data = [headers];
  json.rows.forEach(r => data.push(headers.map(h => r[h] || "")));

  sheet.getRange(1, 1, data.length, data[0].length).setValues(data);
  Logger.log("✅ Загружено строк: " + json.rows.length);
}

===== exportOzonAdsStatsWithCTR =====
function exportOzonAdsStatsWithCTR() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Озон Реклама Итог CTR";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // даты из листа "Настройки"
  const settings = ss.getSheetByName("Настройки");
  const dateFrom = Utilities.formatDate(new Date(settings.getRange("B4").getValue()), "GMT+3", "yyyy-MM-dd");
  const dateTo   = Utilities.formatDate(new Date(settings.getRange("B5").getValue()), "GMT+3", "yyyy-MM-dd");
  const token = getPerformanceToken();

  // --- берём список всех campaignId из листа "Озон Реклама Итог"
  const dailySheet = ss.getSheetByName("Озон Реклама Итог");
  if (!dailySheet) throw new Error("Нет листа 'Озон Реклама Итог'");
  const ids = dailySheet.getRange(2, 1, dailySheet.getLastRow()-1, 1).getValues()
    .map(r => String(r[0]).trim())
    .filter(id => id && id !== "Campaign ID");

  const uniqIds = [...new Set(ids)];
  if (!uniqIds.length) throw new Error("Не найдено campaignId для запроса");

  // --- 1. Генерация отчёта ---
  const urlGenerate = "https://api-performance.ozon.ru/api/client/statistics/json";
  const payload = {
    campaigns: uniqIds,
    dateFrom: dateFrom,
    dateTo: dateTo,
    groupBy: "DATE"
  };

  const respGenerate = UrlFetchApp.fetch(urlGenerate, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const genJson = JSON.parse(respGenerate.getContentText());
  if (!genJson.UUID) throw new Error("Ошибка генерации: " + respGenerate.getContentText());
  const uuid = genJson.UUID;
  Logger.log("📦 UUID: " + uuid);

  // --- 2. Ждём готовности ---
  let state = "NOT_STARTED";
  for (let i = 0; i < 30; i++) {
    const urlStatus = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
    const respStatus = UrlFetchApp.fetch(urlStatus, {
      method: "get",
      headers: { Authorization: "Bearer " + token },
      muteHttpExceptions: true
    });
    const statusJson = JSON.parse(respStatus.getContentText());
    state = statusJson.state || "UNKNOWN";
    Logger.log("⏳ Попытка " + (i+1) + " → state=" + state);

    if (state === "OK") break;
    if (state === "ERROR") throw new Error("❌ Ошибка генерации отчёта");
    Utilities.sleep(3000);
  }
  if (state !== "OK") throw new Error("❌ Отчёт не готов: state=" + state);

  // --- 3. Скачиваем результат ---
  const urlReport = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const respReport = UrlFetchApp.fetch(urlReport, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  const reportJson = JSON.parse(respReport.getContentText());
  const campaigns = Object.keys(reportJson);
  if (!campaigns.length) throw new Error("❌ Пустой отчёт");

  // --- 4. Готовим данные ---
  const headers = [
    "Campaign ID","Campaign Title","Date",
    "SKU","Title","Price ₽",
    "Views","Clicks","CTR %","ToCart",
    "Avg Bid ₽","Spent ₽",
    "Orders","Orders Money ₽",
    "Models","Models Money ₽","DRR %"
  ];
  const result = [headers];

  campaigns.forEach(cid => {
    const c = reportJson[cid];
    const title = c.title;
    const rows = (c.report && c.report.rows) || [];
    rows.forEach(r => {
      result.push([
        cid,
        title,
        r.date || "",
        r.sku || "",
        r.title || "",
        r.price || "",
        r.views || "",
        r.clicks || "",
        r.ctr || "",
        r.toCart || "",
        r.avgBid || "",
        r.moneySpent || "",
        r.orders || "",
        r.ordersMoney || "",
        r.models || "",
        r.modelsMoney || "",
        r.drr || ""
      ]);
    });
  });

  // --- 5. Записываем в таблицу ---
  sheet.getRange(1,1,result.length,result[0].length).setValues(result);
  Logger.log("✅ Загружено строк: " + (result.length-1));
}

===== exportOzonAnalytics =====
function exportOzonAnalytics() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "4 Аналитика";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), "GMT", "yyyy-MM-dd");
  const url = "https://api-seller.ozon.ru/v1/analytics/data";
  const payload = {
    date_from: format(dateFrom),
    date_to: format(dateTo),
    metrics: [
      "revenue","ordered_units","hits_view_search","hits_view_pdp","hits_view",
      "hits_tocart_search","hits_tocart_pdp","session_view_search","session_view_pdp",
      "conv_tocart_search","returns","cancellations","delivered_units","session_view"
    ],
    dimension: ["sku","day"],
    limit: 1000,
    offset: 0
  };

  Logger.log("📡 POST " + url);

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());

  const data = JSON.parse(resp.getContentText()).result?.data || [];
  Logger.log("📦 Получено строк: " + data.length);

  const rows = [[
    "sku","day","revenue — заказано на сумму.","ordered_units — заказано товаров.",
    "hits_view_search — показы в поиске и в категории.","hits_view_pdp — показы на карточке товара.",
    "hits_view — всего показов.","hits_tocart_search — в корзину из поиска или категории.",
    "hits_tocart_pdp — в корзину из карточки товара.","session_view_search — сессии с показом в поиске или в каталоге.",
    "session_view_pdp — сессии с показом на карточке товара.","conv_tocart_search — конверсия в корзину из поиска или категории.",
    "returns — возвращено товаров.","cancellations — отменено товаров.","delivered_units — доставлено товаров.",
    "session_view — всего сессий.","client_id","month"
  ]];

  data.forEach(row => {
    const sku = row.dimensions?.[0]?.id || "";
    const day = row.dimensions?.[1]?.id || "";
    const month = day ? (new Date(day).getMonth() + 1) : "";
    rows.push([
      sku, day,
      row.metrics[0] || 0, row.metrics[1] || 0, row.metrics[2] || 0, row.metrics[3] || 0,
      row.metrics[4] || 0, row.metrics[5] || 0, row.metrics[6] || 0, row.metrics[7] || 0,
      row.metrics[8] || 0, row.metrics[9] || 0, row.metrics[10] || 0, row.metrics[11] || 0,
      row.metrics[12] || 0, row.metrics[13] || 0,
      clientId, month
    ]);
  });

  if (rows.length > 1) {
    sheet.getRange(1,1,rows.length,rows[0].length).setValues(rows);
  }
  Logger.log("✅ Данные записаны в лист '" + sheetName + "'");
}

===== exportOzonProducts =====
function exportOzonProducts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");

  const clientId = settings.getRange("B2").getValue();
  const apiKey = settings.getRange("B3").getValue();

  Logger.log("🔑 ClientId: " + clientId);
  Logger.log("🔑 ApiKey: " + apiKey);

  const sheetName = "3 Товары";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // Заголовки
  sheet.appendRow([
    "Артикул",
    "ID товара",
    "Остатки FBO",
    "Остатки FBS",
    "Архив",
    "Со скидкой"
  ]);

  let last_id = "";
  let totalExported = 0;

  do {
    const payload = { limit: 1000, filter: { visibility: "ALL" } };
    if (last_id) payload.last_id = last_id;

    const response = UrlFetchApp.fetch("https://api-seller.ozon.ru/v3/product/list", {
      method: "post",
      contentType: "application/json",
      headers: {
        "Client-Id": String(clientId),
        "Api-Key": String(apiKey)
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();
    if (code !== 200) {
      throw new Error("Ошибка HTTP " + code + ": " + response.getContentText());
    }

    const data = JSON.parse(response.getContentText());
    const items = data.result?.items || [];
    Logger.log("📦 Получено товаров: " + items.length);

    if (items.length > 0) {
      const rows = items.map(item => [
        item.offer_id || "",
        item.product_id || "",
        item.has_fbo_stocks || false,
        item.has_fbs_stocks || false,
        item.archived || false,
        item.is_discounted || false
      ]);
      sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
    }

    totalExported += items.length;
    last_id = data.result?.last_id || "";
  } while (last_id);

  Logger.log("🏁 Всего выгружено: " + totalExported);
}

===== exportOzonStocks =====
function exportOzonStocks() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Остатки";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();

  Logger.log("🔑 ClientId: " + clientId);

  // Заголовки
  const headers = [
    "sku","offer_id","name",
    "warehouse_id","warehouse_name","cluster_id","cluster_name",
    "available_stock_count","valid_stock_count","expiring_stock_count","excess_stock_count",
    "stock_defect_stock_count","transit_defect_stock_count","other_stock_count","transit_stock_count",
    "requested_stock_count","return_from_customer_stock_count","return_to_seller_stock_count","waiting_docs_stock_count",
    "ads","ads_cluster","days_without_sales","days_without_sales_cluster",
    "idc","idc_cluster","turnover_grade","turnover_grade_cluster"
  ];
  sheet.appendRow(headers);

  // 1. Собираем все product_id
  let allProductIds = [];
  let last_id = "";
  do {
    const payload = { limit: 1000, filter: { visibility: "ALL" }, last_id };
    const resp = UrlFetchApp.fetch("https://api-seller.ozon.ru/v3/product/list", {
      method: "post",
      contentType: "application/json",
      headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());
    const data = JSON.parse(resp.getContentText()).result;
    const items = data?.items || [];

    items.forEach(it => { if (it.product_id) allProductIds.push(String(it.product_id)); });
    last_id = data?.last_id || "";
  } while (last_id);

  Logger.log("📦 Получено product_id: " + allProductIds.length);

  // 2. Получаем SKU по product_id (батчами по 1000)
  let allSkus = [];
  for (let i = 0; i < allProductIds.length; i += 1000) {
    const batch = allProductIds.slice(i, i + 1000);
    const payload = { product_id: batch };

    const resp = UrlFetchApp.fetch("https://api-seller.ozon.ru/v3/product/info/list", {
      method: "post",
      contentType: "application/json",
      headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());
    const items = JSON.parse(resp.getContentText()).items || [];
    items.forEach(it => { if (it.sku) allSkus.push(String(it.sku)); });
  }

  Logger.log("✅ Всего SKU получено: " + allSkus.length);

  // 3. Тянем остатки по SKU батчами по 100
  for (let i = 0; i < allSkus.length; i += 100) {
    const batch = allSkus.slice(i, i + 100);
    const payload = { skus: batch };

    const resp = UrlFetchApp.fetch("https://api-seller.ozon.ru/v1/analytics/stocks", {
      method: "post",
      contentType: "application/json",
      headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());
    const items = JSON.parse(resp.getContentText()).items || [];

    const rows = items.map(it => [
      it.sku || "",
      it.offer_id || "",
      it.name || "",
      it.warehouse_id || "",
      it.warehouse_name || "",
      it.cluster_id || "",
      it.cluster_name || "",
      it.available_stock_count || 0,
      it.valid_stock_count || 0,
      it.expiring_stock_count || 0,
      it.excess_stock_count || 0,
      it.stock_defect_stock_count || 0,
      it.transit_defect_stock_count || 0,
      it.other_stock_count || 0,
      it.transit_stock_count || 0,
      it.requested_stock_count || 0,
      it.return_from_customer_stock_count || 0,
      it.return_to_seller_stock_count || 0,
      it.waiting_docs_stock_count || 0,
      it.ads || 0,
      it.ads_cluster || 0,
      it.days_without_sales || 0,
      it.days_without_sales_cluster || 0,
      it.idc || 0,
      it.idc_cluster || 0,
      it.turnover_grade || "",
      it.turnover_grade_cluster || ""
    ]);

    if (rows.length) {
      sheet.getRange(sheet.getLastRow()+1, 1, rows.length, rows[0].length).setValues(rows);
    }

    Logger.log("📦 SKU " + (i+1) + "–" + (i+batch.length) + " выгружено, строк: " + rows.length);
  }

  Logger.log("🏁 Остатки записаны в лист '" + sheetName + "'");
}

===== exportOzonTransactionTotals =====
function exportOzonTransactionTotals() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Транзакции Итоги";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // Читаем настройки
  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), "GMT", "yyyy-MM-dd'T'HH:mm:ss'Z'");
  const fromIso = format(dateFrom);
  const toIso   = format(dateTo);

  Logger.log("🔑 ClientId: " + clientId);
  Logger.log("🔑 ApiKey: " + apiKey);
  Logger.log("📅 Период: " + fromIso + " → " + toIso);

  const url = "https://api-seller.ozon.ru/v3/finance/transaction/totals";
  const payload = {
    date: { from: fromIso, to: toIso },
    posting_number: "",
    transaction_type: "all"
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "Client-Id": String(clientId),
      "Api-Key": String(apiKey),
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const code = resp.getResponseCode();
  Logger.log("🔎 Код ответа: " + code);
  if (code !== 200) throw new Error(resp.getContentText());

  const result = JSON.parse(resp.getContentText()).result || {};
  Logger.log("📦 Получено: " + JSON.stringify(result));

  // Заголовки
  const headers = [
    "accruals_for_sale — сумма заказов и возвратов",
    "sale_commission — удержанные комиссии",
    "processing_and_delivery — логистика и обработка",
    "refunds_and_cancellations — возвраты и отмены",
    "services_amount — услуги",
    "compensation_amount — компенсации",
    "money_transfer — переводы за доставку",
    "others_amount — прочие начисления",
    "client_id",
    "date_from",
    "date_to"
  ];
  sheet.appendRow(headers);

  // Данные
  sheet.appendRow([
    result.accruals_for_sale || 0,
    result.sale_commission || 0,
    result.processing_and_delivery || 0,
    result.refunds_and_cancellations || 0,
    result.services_amount || 0,
    result.compensation_amount || 0,
    result.money_transfer || 0,
    result.others_amount || 0,
    clientId,
    fromIso,
    toIso
  ]);

  Logger.log("✅ Данные итогов транзакций записаны в лист '" + sheetName + "'");
}

===== exportOzonTransactions =====
function exportOzonTransactions() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Транзакции";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  // Читаем настройки
  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = (d) => Utilities.formatDate(new Date(d), "GMT", "yyyy-MM-dd'T'HH:mm:ss'Z'");
  const fromIso = format(dateFrom);
  const toIso   = format(dateTo);

  Logger.log("🔑 ClientId: " + clientId);
  Logger.log("🔑 ApiKey: " + apiKey);
  Logger.log("📅 Период: " + fromIso + " → " + toIso);

  // Заголовки
  const headers = [
    "operation_id",
    "operation_type",
    "operation_type_name",
    "operation_date",
    "amount",
    "type",
    "posting_number",
    "delivery_schema",
    "order_date",
    "warehouse_id",
    "sku",
    "item_name",
    "service_name",
    "service_price"
  ];
  sheet.appendRow(headers);

  let page = 1;
  const pageSize = 1000;
  let totalRows = 0;
  const allRows = [];

  while (true) {
    const url = "https://api-seller.ozon.ru/v3/finance/transaction/list";
    const payload = {
      filter: {
        date: { from: fromIso, to: toIso },
        transaction_type: "all"
      },
      page: page,
      page_size: pageSize
    };

    Logger.log("📡 POST " + url + " page=" + page);
    const resp = UrlFetchApp.fetch(url, {
      method: "post",
      contentType: "application/json",
      headers: {
        "Client-Id": String(clientId),
        "Api-Key": String(apiKey)
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });

    const code = resp.getResponseCode();
    const text = resp.getContentText();
    Logger.log("🔎 Код ответа: " + code);

    if (code !== 200) {
      Logger.log("❌ Ошибка: " + text);
      break;
    }

    const json = JSON.parse(text).result;
    if (!json || !json.operations || json.operations.length === 0) {
      Logger.log("⚠️ Данных больше нет.");
      break;
    }

    const batch = json.operations.map(op => {
      const sku = op.items && op.items.length ? op.items[0].sku : "";
      const itemName = op.items && op.items.length ? op.items[0].name : "";
      const serviceName = op.services && op.services.length ? op.services[0].name : "";
      const servicePrice = op.services && op.services.length ? op.services[0].price : "";

      return [
        op.operation_id || "",
        op.operation_type || "",
        op.operation_type_name || "",
        op.operation_date || "",
        op.amount || 0,
        op.type || "",
        op.posting?.posting_number || "",
        op.posting?.delivery_schema || "",
        op.posting?.order_date || "",
        op.posting?.warehouse_id || "",
        sku,
        itemName,
        serviceName,
        servicePrice
      ];
    });

    allRows.push(...batch);

    totalRows += json.operations.length;
    Logger.log("📦 Получено операций: " + json.operations.length + ", всего: " + totalRows);

    if (page >= json.page_count) break;
    page++;
  }

  if (allRows.length > 0) {
    sheet.getRange(2, 1, allRows.length, allRows[0].length).setValues(allRows);
  }

  Logger.log("✅ Данные транзакций записаны в лист '" + sheetName + "'. Всего строк: " + totalRows);
}

===== getOzonSettings =====
function getOzonSettings() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Настройки");
  if (!sheet) throw new Error("Создайте лист 'Настройки'");

  // читаем 7 строк (Client-Id, Api-Key, даты, сервисный аккаунт, client, secret)
  const values = sheet.getRange(2, 1, 7, 2).getValues();

  const clientId = values[0][1];
  const apiKey = values[1][1];
  const dateFrom = values[2][1];
  const dateTo = values[3][1];
  const serviceAccount = values[4][1];
  const clientPerf = values[5][1];
  const secretPerf = values[6][1];

  return { clientId, apiKey, dateFrom, dateTo, serviceAccount, clientPerf, secretPerf };
}

===== getPerformanceToken =====
function getPerformanceToken() {
  const clientPerf = getSettingValue("Client Performance");
  const secretPerf = getSettingValue("Secret Performance");

  const props = PropertiesService.getScriptProperties();
  const lastTime = props.getProperty("perfTokenTime");
  const token = props.getProperty("perfToken");
  const currentTime = new Date().getTime();

  if (token && lastTime && currentTime - parseInt(lastTime) < 1800000) {
    Logger.log("Используем сохранённый токен");
    return token;
  }

  const payload = {
    client_id: clientPerf,
    client_secret: secretPerf,
    grant_type: "client_credentials"
  };

  const resp = UrlFetchApp.fetch("https://api-performance.ozon.ru/api/client/token", {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const json = JSON.parse(resp.getContentText());
  if (!json.access_token) throw new Error("Ошибка токена: " + resp.getContentText());

  props.setProperty("perfToken", json.access_token);
  props.setProperty("perfTokenTime", String(currentTime));

  Logger.log("Новый токен получен");
  return json.access_token;
}

===== getSettingValue =====
function getSettingValue(keyName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Настройки");
  if (!sheet) throw new Error("Нет листа 'Настройки'");

  const values = sheet.getRange(1, 1, sheet.getLastRow(), 2).getValues();
  const row = values.find(r => String(r[0]).trim() === keyName);
  if (!row) throw new Error("Ключ не найден: " + keyName);
  return row[1];
}

===== onOpen =====
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("📦 OZON")
    .addItem("📥 Скачать товары", "exportOzonProducts")
    .addItem("📊 Скачать аналитику", "exportOzonAnalytics")
    .addItem("💰 Скачать транзакции за период", "exportOzonTransactions")
    .addItem("📑 Скачать итоги транзакций", "exportOzonTransactionTotals")
    .addItem("📦 Скачать остатки", "exportOzonStocks")
    .addSeparator()
    .addItem("📈 Реклама — дневная", "exportOzonAdsDaily")
    .addItem("📈 Реклама — отчёт Продукты", "exportOzonAdsProductsReport")
    .addItem("📈 Реклама — отчёт Заказы", "exportOzonAdsOrdersReport")
    .addItem("📈 Реклама — кампании", "exportOzonAdsCampaigns")
    .addItem("📉 Реклама — расходы", "exportOzonAdsExpense")
    .addToUi();
}

===== parseNumber =====
function parseNumber(val) {
  if (!val) return 0;
  if (typeof val === "number") return val;
  return Number(val.toString().replace(",",".").replace(/\s/g,"")) || 0;
}

===== parseOrderDate =====
function parseOrderDate(d) {
  if (!d) return "";
  if (Object.prototype.toString.call(d) === "[object Date]") {
    return Utilities.formatDate(d, "GMT+2", "yyyy-MM-dd");
  }
  const parts = d.toString().split(".");
  if (parts.length === 3) {
    return `${parts[2]}-${parts[1].padStart(2,"0")}-${parts[0].padStart(2,"0")}`;
  }
  return d;
}

===== testAdsHeaders =====
function testAdsHeaders() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  const sheets = [
    "Озон Реклама Итог",
    "Озон Реклама Продукты",
    "Озон Реклама Заказы"
  ];

  sheets.forEach(name => {
    const sh = ss.getSheetByName(name);
    if (!sh) {
      Logger.log("❌ Нет листа: " + name);
      return;
    }
    const headers = sh.getDataRange().getValues()[0];
    Logger.log("✅ " + name + " — " + JSON.stringify(headers));
  });
}

===== testAnalyticsShort =====
function testAnalyticsShort() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();

  const url = "https://api-seller.ozon.ru/v1/analytics/data";
  const payload = {
    date_from: "2025-07-01",
    date_to: "2025-07-05",
    metrics: ["revenue","ordered_units"],
    dimension: ["sku","day"],
    limit: 5,
    offset: 0
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "Client-Id": String(clientId),
      "Api-Key": String(apiKey)
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код: " + resp.getResponseCode());
  Logger.log("Ответ: " + resp.getContentText());
}

===== testCheckOzonAdsOrdersReport =====
function testCheckOzonAdsOrdersReport() {
  const token = getPerformanceToken();
  const uuid = "3c57463a-3972-440a-b4fc-a2f3d896f277"; // UUID из прошлого шага

  const url = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText());
}

===== testDownloadOzonAdsOrdersReport =====
function testDownloadOzonAdsOrdersReport() {
  const token = getPerformanceToken();
  const uuid = "3c57463a-3972-440a-b4fc-a2f3d896f277"; // UUID из шага 1

  const url = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Первые 500 символов: " + resp.getContentText().slice(0, 500));
}

===== testGetSkus =====
function testGetSkus() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "SKU";
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  sheet.clear();

  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();

  // 1. Собираем product_id
  let last_id = "";
  let productIds = [];
  do {
    const payload = { limit: 1000, last_id: last_id, filter: { visibility: "ALL" } };
    const resp = UrlFetchApp.fetch("https://api-seller.ozon.ru/v3/product/list", {
      method: "post",
      contentType: "application/json",
      headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });
    if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());
    const data = JSON.parse(resp.getContentText()).result;
    const items = data?.items || [];
    items.forEach(it => productIds.push(it.product_id));
    last_id = data?.last_id || "";
  } while (last_id);

  Logger.log("📦 Получено product_id: " + productIds.length);

  // 2. Берём SKU по product_id батчами по 1000
  let allSkus = [];
  for (let i = 0; i < productIds.length; i += 1000) {
    const batch = productIds.slice(i, i + 1000);
    const payloadInfo = { product_id: batch };
    const resp = UrlFetchApp.fetch("https://api-seller.ozon.ru/v3/product/info/list", {
      method: "post",
      contentType: "application/json",
      headers: { "Client-Id": String(clientId), "Api-Key": String(apiKey) },
      payload: JSON.stringify(payloadInfo),
      muteHttpExceptions: true
    });
    if (resp.getResponseCode() !== 200) throw new Error(resp.getContentText());
    const items = JSON.parse(resp.getContentText()).items || [];
    items.forEach(it => { if (it.sku) allSkus.push(it.sku); });
  }

  Logger.log("✅ Всего SKU получено: " + allSkus.length);

  if (allSkus.length) {
    sheet.getRange(1, 1, allSkus.length, 1).setValues(allSkus.map(s => [s]));
  }
}

===== testOzonAdsCampaigns =====
function testOzonAdsCampaigns() {
  const token = getPerformanceToken(); // берём токен из 8 Реклама Настройки
  const url = "https://api-performance.ozon.ru/api/client/campaign?page=1&pageSize=50";

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText());
}

===== testOzonAdsCampaignsIDs =====
function testOzonAdsCampaignsIDs() {
  const token = getPerformanceToken(); // берём токен из 8 Реклама Настройки
  const url = "https://api-performance.ozon.ru/api/client/campaign?page=1&pageSize=100";

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  if (resp.getResponseCode() !== 200) {
    throw new Error("Ошибка HTTP " + resp.getResponseCode() + ": " + resp.getContentText());
  }

  const json = JSON.parse(resp.getContentText());
  const list = json.list || [];
  Logger.log("📊 Получено кампаний: " + list.length);

  list.forEach(c => {
    Logger.log("ID: " + c.id + " | Title: " + c.title + " | State: " + c.state);
  });
}

===== testOzonAdsDailyRaw =====
function testOzonAdsDailyRaw() {
  const settings = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Настройки");
  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const fromStr = format(dateFrom);
  const toStr   = format(dateTo);

  const token = getPerformanceToken();

  const url = "https://api-performance.ozon.ru/api/client/statistics/daily/json"
    + "?dateFrom=" + fromStr + "&dateTo=" + toStr;

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("RAW JSON: " + resp.getContentText().slice(0, 2000)); // первые 2000 символов
}

===== testOzonAdsExpense =====
function testOzonAdsExpense() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  if (!settings) throw new Error("Нет листа 'Настройки'");

  const dateFrom = settings.getRange("B4").getValue();
  const dateTo   = settings.getRange("B5").getValue();

  const format = d => Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), "yyyy-MM-dd");
  const fromStr = format(dateFrom);
  const toStr   = format(dateTo);

  const token = getPerformanceToken();

  const url = "https://api-performance.ozon.ru/api/client/statistics/expense/json" +
              "?dateFrom=" + fromStr +
              "&dateTo=" + toStr;

  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText().slice(0, 1000));
}

===== testOzonAdsOrdersReport =====
function testOzonAdsOrdersReport() {
  const settings = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Настройки");
  const dateFrom = new Date(settings.getRange("B4").getValue());
  const dateTo   = new Date(settings.getRange("B5").getValue());
  const token = getPerformanceToken();

  const url = "https://api-performance.ozon.ru/api/client/statistic/orders/generate/json";
  const payload = { 
    from: dateFrom.toISOString(), 
    to: dateTo.toISOString() 
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText());
}

===== testOzonAdsProductsReport =====
function testOzonAdsProductsReport() {
  const token = getPerformanceToken();

  // даты тестовые (возьми август, чтобы совпадало с остальными)
  const from = "2025-08-01T00:00:00Z";
  const to   = "2025-08-31T23:59:59Z";

  // --- 1. генерируем отчёт
  const urlGen = "https://api-performance.ozon.ru/api/client/statistic/products/generate/json";
  const respGen = UrlFetchApp.fetch(urlGen, {
    method: "post",
    headers: { 
      Authorization: "Bearer " + token,
      "Content-Type": "application/json"
    },
    payload: JSON.stringify({ from, to }),
    muteHttpExceptions: true
  });

  if (respGen.getResponseCode() !== 200) {
    throw new Error("Ошибка генерации: " + respGen.getContentText());
  }

  const uuid = JSON.parse(respGen.getContentText()).UUID;
  Logger.log("🟢 UUID: " + uuid);

  // --- 2. проверяем отчёт по UUID
  const urlReport = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  Utilities.sleep(3000); // ждём 3 сек чтобы успел сформироваться
  const respReport = UrlFetchApp.fetch(urlReport, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + respReport.getResponseCode());
  Logger.log("Первые 1000 символов: " + respReport.getContentText().substring(0, 1000));
}

===== testOzonAdsProductsStep1 =====
function testOzonAdsProductsStep1() {
  const settings = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Настройки");
  const dateFrom = new Date(settings.getRange("B4").getValue());
  const dateTo   = new Date(settings.getRange("B5").getValue());
  const token = getPerformanceToken();

  const url = "https://api-performance.ozon.ru/api/client/statistic/products/generate/json";
  const payload = { from: dateFrom.toISOString(), to: dateTo.toISOString() };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: { Authorization: "Bearer " + token },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText());
}

===== testOzonAdsProductsStep2 =====
function testOzonAdsProductsStep2() {
  const token = getPerformanceToken();
  const uuid = "___ПОДСТАВЬ_UUID_ИЗ_STEP1___";

  const url = "https://api-performance.ozon.ru/api/client/statistics/" + uuid;
  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Тело ответа: " + resp.getContentText());
}

===== testOzonAdsProductsStep3 =====
function testOzonAdsProductsStep3() {
  const token = getPerformanceToken();
  const uuid = "___ПОДСТАВЬ_UUID_ИЗ_STEP1___";

  const url = "https://api-performance.ozon.ru/api/client/statistics/report?UUID=" + uuid;
  const resp = UrlFetchApp.fetch(url, {
    method: "get",
    headers: { Authorization: "Bearer " + token },
    muteHttpExceptions: true
  });

  Logger.log("Код ответа: " + resp.getResponseCode());
  Logger.log("Первые 500 символов: " + resp.getContentText().slice(0, 500));
}

===== testOzonStocks =====
function testOzonStocks() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();

  const url = "https://api-seller.ozon.ru/v1/analytics/stocks";
  const payload = {
    skus: ["1805436617","2104149519","1939919001"], // ⚡️ подставляем твои SKU
    item_tags: ["ITEM_ATTRIBUTE_NONE"],
    turnover_grades: [],
    cluster_ids: [],
    warehouse_ids: []
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "Client-Id": String(clientId),
      "Api-Key": String(apiKey)
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код: " + resp.getResponseCode());
  Logger.log("Ответ: " + resp.getContentText());
}

===== testOzonTransactionTotals =====
function testOzonTransactionTotals() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();
  const dateFrom = new Date("2025-07-01T00:00:00Z");
  const dateTo   = new Date("2025-07-31T23:59:59Z");

  const url = "https://api-seller.ozon.ru/v3/finance/transaction/totals";
  const payload = {
    date: {
      from: dateFrom.toISOString(),
      to: dateTo.toISOString()
    },
    posting_number: "",
    transaction_type: "all"
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "Client-Id": String(clientId),
      "Api-Key": String(apiKey),
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код: " + resp.getResponseCode());
  Logger.log("Ответ: " + resp.getContentText());
}

===== testTransactionsList =====
function testTransactionsList() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Настройки");
  const clientId = settings.getRange("B2").getValue();
  const apiKey   = settings.getRange("B3").getValue();

  const url = "https://api-seller.ozon.ru/v3/finance/transaction/list";
  const payload = {
    filter: {
      date: {
        from: "2025-07-01T00:00:00.000Z",
        to:   "2025-07-02T00:00:00.000Z"
      },
      transaction_type: "all"
    },
    page: 1,
    page_size: 5
  };

  const resp = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "Client-Id": String(clientId),
      "Api-Key": String(apiKey)
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  Logger.log("Код: " + resp.getResponseCode());
  Logger.log("Ответ: " + resp.getContentText());
}
