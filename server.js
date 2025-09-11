// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
const axios = require('axios');
const dayjs = require('dayjs');

// ── bcAuth: รองรับทั้ง default และ named export ───────────────────────────
const _bcAuth = require('./bcAuth');
const getBcAccessToken =
  (_bcAuth && _bcAuth.getBcAccessToken) || // named export
  (_bcAuth && _bcAuth.default) ||          // default export
  _bcAuth;                                 // module.exports = fn

const app = express();
const PORT = process.env.PORT || 5000;

// ─────────────────────────────────────────────────────────────────────────────
// Core middlewares
app.use(express.json());
app.use(cors({ origin: '*', credentials: true }));

// ─────────────────────────────────────────────────────────────────────────────
// Helpers (ชุดเดียวพอ)
function chunkArray(arr, size = 30) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
function buildDocumentNoFilter(docNos = []) {
  if (!docNos.length) return '';
  const cond = docNos
    .map(no => `Document_No eq '${String(no).replace(/'/g, "''")}'`)
    .join(' or ');
  return `$filter=${cond}`;
}
// ดึงทุกหน้า (@odata.nextLink)
async function fetchAllOData(url, headers) {
  let urlNext = url;
  let all = [];
  while (urlNext) {
    const { data } = await axios.get(urlNext, { headers });
    all = all.concat(data?.value || []);
    urlNext = data?.['@odata.nextLink'] || null;
  }
  return all;
}
const dt = iso => iso; // คืน ISO ตรง ๆ (OData v4 เปรียบเทียบแบบไม่ต้องครอบ quote)

// ─────────────────────────────────────────────────────────────────────────────
// Unified handler (รองรับทั้ง GET/POST และไม่ต้องส่ง body ก็ได้)
async function bcDataHandler(req, res) {
  try {
    // 1) รับพารามิเตอร์
    const nowYear = new Date().getFullYear();
    const q = req.query || {};
    const b = req.body || {};
    const selectedYear = Number(q.year ?? b.year ?? nowYear);

    const rawMonth = q.month ?? b.month ?? null;
    const selectedMonth = rawMonth != null && rawMonth !== ''
      ? Number(rawMonth)
      : null;

    const branchParam = (q.branch ?? b.branch ?? '').toString().trim();
    const branch = branchParam || ''; // ใส่ 'URY' ถ้าต้องการ default branch

    // 2) ช่วงวันเวลา
    let start = dayjs(`${selectedYear}-01-01T00:00:00.000Z`);
    let end = dayjs(`${selectedYear}-12-31T23:59:59.999Z`);
    if (Number.isFinite(selectedMonth) && selectedMonth >= 1 && selectedMonth <= 12) {
      const padded = String(selectedMonth).padStart(2, '0');
      start = dayjs(`${selectedYear}-${padded}-01T00:00:00.000Z`);
      end = start.endOf('month');
    }

    // 3) ขอ token
    if (typeof getBcAccessToken !== 'function') {
      return res.status(500).json({ error: 'getBcAccessToken not resolvable as a function (ตรวจรูปแบบ export/import ของ bcAuth.js)' });
    }
    const token = await getBcAccessToken();

    // 4) ดึง SO จาก BC (ดึงทุกหน้า)
    const company = String(process.env.BC_COMPANY_NAME || '').replace(/'/g, "''");
    const base = `https://api.businesscentral.dynamics.com/v2.0/${process.env.BC_TENANT_ID}/${process.env.BC_ENVIRONMENT}/ODataV4/Company('${company}')`;

    const orderUrl =
      `${base}/ServiceOrderList?$orderby=Order_Date desc` +
      `&$filter=Status eq 'pending' and Order_Date ge ${dt(start.toISOString())} and ` +
      `Order_Date le ${dt(end.toISOString())} and Service_Order_Type ne 'ADD'`;

    const allOrders = await fetchAllOData(orderUrl, {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json'
    });

    // 5) เลขที่ SO ที่มีแล้วใน DB ปีเดียวกัน (ตาราง u_inspection, คอลัมน์ sv)
    const existingOrders = await new Promise((resolve, reject) => {
      db.query(
        'SELECT sv FROM u_inspection WHERE YEAR(incoming_date) = ?',
        [selectedYear],
        (err, rows) => err ? reject(err) : resolve(rows.map(r => r.sv))
      );
    });
    const existingSet = new Set(existingOrders);

    // 6) ฟิลเตอร์รายการใหม่ + กรอง branch (ถ้าระบุ)
    const filteredOrders = allOrders.filter(o => {
      const notExists = !existingSet.has(o.No);
      const oBranch = o.USVT_ResponsibilityCenter || o.Responsibility_Center || null;
      const matchBranch = !branch || (oBranch === branch);
      return notExists && matchBranch;
    });

    if (!filteredOrders.length) return res.json([]);

    // 7) ดึง ServiceItemLines ทีละชุด (ดึงทุกหน้า)
    const orderNos = filteredOrders.map(o => o.No);
    const orderChunks = chunkArray(orderNos, 30);

    let allItems = [];
    for (const chunk of orderChunks) {
      const filter = buildDocumentNoFilter(chunk);
      const itemUrl = `${base}/ServiceItemLines?${filter}`;
      const part = await fetchAllOData(itemUrl, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      allItems = allItems.concat(part);
    }

    // 8) join คร่าว ๆ (หยิบตัวแรก)
    const joined = filteredOrders.map(order => {
      const related = allItems.filter(it => it.Document_No === order.No);
      return {
        ...order,
        Service_Item_No: related[0]?.Service_Item_No || '',
        Item_No: related[0]?.Item_No || ''
      };
    });

    res.json(joined);
  } catch (err) {
    console.error('BC API JOIN Error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'เกิดข้อผิดพลาดในการดึงข้อมูลจาก BC' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// /api/sv/motor — join โดยอิงคีย์ No/Document_No ตามที่กำหนด (ไม่ใช้ branch)
async function svMotorHandler(req, res) {
  try {
    const nowYear = new Date().getFullYear();
    const q = req.query || {};
    const b = req.body || {};
    const selectedYear = Number(q.year ?? b.year ?? nowYear);

    const startISO = `${selectedYear}-01-01T00:00:00.000Z`;
    const endISO = `${selectedYear}-12-31T23:59:59.999Z`;

    if (typeof getBcAccessToken !== 'function') {
      return res.status(500).json({ error: 'getBcAccessToken not resolvable' });
    }
    const token = await getBcAccessToken();

    const company = String(process.env.BC_COMPANY_NAME || '').replace(/'/g, "''");
    const base =
      `https://api.businesscentral.dynamics.com/v2.0/${process.env.BC_TENANT_ID}/${process.env.BC_ENVIRONMENT}` +
      `/ODataV4/Company('${company}')`;

    // 1) หัวรายการจาก Service_Order_Excel (ใช้ No เป็นคีย์อ้างอิง, กรองปีด้วย Order_Date) — ดึงทุกหน้า
    const headerSelect = ['No', 'Order_Date', 'USVT_Job_Scope'].join(',');
    const headerFilter = `(Order_Date ge ${startISO} and Order_Date le ${endISO})`;
    const headerUrl =
      `${base}/Service_Order_Excel?$select=${headerSelect}&$filter=${encodeURI(headerFilter)}&$orderby=Order_Date desc`;

    const headers = await fetchAllOData(headerUrl, {
      Authorization: `Bearer ${token}`,
      Accept: 'application/json'
    });
    if (!headers.length) return res.json([]);

    const svList = headers.map(h => h.No).filter(Boolean);
    const chunks = chunkArray(svList, 30);

    // 2) ServiceItemLines — ดึงทุกหน้า, join ด้วย Document_No
    let itemLines = [];
    for (const chunk of chunks) {
      const filter = buildDocumentNoFilter(chunk);
      const url =
        `${base}/ServiceItemLines?${filter}` +
        `&$select=Document_No,Service_Item_No,Description`;
      const part = await fetchAllOData(url, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      itemLines = itemLines.concat(part);
    }
    const itemsByDoc = new Map();
    for (const it of itemLines) {
      const k = it.Document_No;
      if (!itemsByDoc.has(k)) itemsByDoc.set(k, []);
      itemsByDoc.get(k).push(it);
    }

    // 3) ServiceOrderLines — ดึงทุกหน้า, join ด้วย Document_No
    let orderLines = [];
    for (const chunk of chunks) {
      const filter = buildDocumentNoFilter(chunk);
      const url =
        `${base}/ServiceOrderLines?${filter}` +
        `&$select=Document_No,USVT_Ref_Sales_Quote_No,ServiceItemNo,USVT_Percent_of_Completion`;
      const part = await fetchAllOData(url, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      orderLines = orderLines.concat(part);
    }
    const orderByDoc = new Map();
    for (const ol of orderLines) {
      const k = ol.Document_No;
      if (!orderByDoc.has(k)) orderByDoc.set(k, []);
      orderByDoc.get(k).push(ol);
    }

    // 4) ดึง mapping ภายใน u_inspection + u_form
    const localRows = await new Promise((resolve, reject) => {
      db.query(
        `
        SELECT i.sv, i.type_form, f.form_name
        FROM u_inspection i
        LEFT JOIN u_form f ON i.type_form = f.id
        WHERE YEAR(i.incoming_date) = ?
        `,
        [selectedYear],
        (err, rows) => (err ? reject(err) : resolve(rows || []))
      );
    });
    const localBySv = new Map();
    for (const r of localRows) {
      localBySv.set(r.sv, { type_form: r.type_form, form_name: r.form_name });
    }

    // 5) รวมผลลัพธ์ให้ 1 แถวต่อ SV (No)
    const result = headers.map(h => {
      const sv = h.No;
      const itemArr = itemsByDoc.get(sv) || [];
      const orderArr = orderByDoc.get(sv) || [];

      const firstItem = itemArr[0] || {};
      const service_item_no = firstItem.ServiceItemNo || firstItem.Service_Item_No || '';
      const local = localBySv.get(sv) || { type_form: null, form_name: null };

      return {
        // คีย์หลักและเมตต้า
        sv, // = Service_Order_Excel.No
        order_date: h.Order_Date || null,
        job_scope: h.USVT_Job_Scope ?? null,

        // จาก ServiceItemLines (หลักตามที่ต้องการ)
        ref_sales_quote_no: firstItem.USVT_Ref_Sales_Quote_No ?? null,
        service_item_no,
        percent_complete: firstItem.USVT_Percent_of_Completion ?? null,
        repair_status: firstItem.Repair_Status_Code ?? null,

        // จากระบบภายใน
        type_form: local.type_form,
        form_name: local.form_name,

        // แนบรายละเอียดบรรทัด (ถ้าอยากย่อเป็น count แทน เปลี่ยนตรงนี้ได้)
        service_item_lines: itemArr,
        service_order_lines: orderArr
      };
    });

    res.json(result);
  } catch (err) {
    console.error('SV MOTOR Error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'เกิดข้อผิดพลาดในการรวมข้อมูล motor' });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Routes
app.get('/api/bc/data', bcDataHandler);
app.post('/api/bc/data', bcDataHandler);

app.get('/api/sv/motor', svMotorHandler);
app.post('/api/sv/motor', svMotorHandler);

// ─────────────────────────────────────────────────────────────────────────────
// Utilities ใช้ร่วมกัน
function startEndISO(year, month) {
  if (month && month >= 1 && month <= 12) {
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 0, 23, 59, 59, 999));
    return { startISO: start.toISOString(), endISO: end.toISOString() };
  }
  return {
    startISO: `${year}-01-01T00:00:00.000Z`,
    endISO: `${year}-12-31T23:59:59.999Z`
  };
}

async function getBcBaseAndToken() {
  const token = await getBcAccessToken();
  const company = String(process.env.BC_COMPANY_NAME || '').replace(/'/g, "''");
  const base =
    `https://api.businesscentral.dynamics.com/v2.0/${process.env.BC_TENANT_ID}/${process.env.BC_ENVIRONMENT}` +
    `/ODataV4/Company('${company}')`;
  return { token, base };
}

// ดึงหัว Service_Order_Excel ตามช่วงวัน (คืน array ของ record ที่มี No/Order_Date/USVT_Job_Scope)
async function fetchHeadersByPeriod(base, token, startISO, endISO) {
  const headerSelect = ['No', 'Order_Date', 'USVT_Job_Scope'].join(',');
  const headerFilter = `(Order_Date ge ${startISO} and Order_Date le ${endISO})`;
  const headerUrl =
    `${base}/Service_Order_Excel?$select=${headerSelect}&$filter=${encodeURI(headerFilter)}&$orderby=Order_Date desc`;
  const headers = await fetchAllOData(headerUrl, {
    Authorization: `Bearer ${token}`,
    Accept: 'application/json'
  });
  return headers || [];
}

// ─────────────────────────────────────────────────────────────────────────────
// BI 1) SUMMARY: 1 แถวต่อ SV (ไม่มี arrays) — สำหรับตารางหลัก
// GET /api/bi/sv-motor?year=2025&month=1&page=1&limit=1000
app.get('/api/bi/sv-motor', async (req, res) => {
  try {
    const nowYear = new Date().getFullYear();
    const year = Number(req.query.year ?? nowYear);
    const month = req.query.month ? Number(req.query.month) : null;
    const page = Math.max(1, Number(req.query.page ?? 1));
    const limit = Math.min(5000, Math.max(100, Number(req.query.limit ?? 1000)));

    const { startISO, endISO } = startEndISO(year, month);
    const { token, base } = await getBcBaseAndToken();

    // หัวรายการ (SV list)
    const headers = await fetchHeadersByPeriod(base, token, startISO, endISO);
    if (!headers.length) return res.json({ page, limit, nextPage: null, rows: [] });

    const svList = headers.map(h => h.No).filter(Boolean);
    const startIdx = (page - 1) * limit;
    const endIdx = Math.min(svList.length, startIdx + limit);
    const svSlice = svList.slice(startIdx, endIdx);
    const nextPage = endIdx < svList.length ? page + 1 : null;

    // ดึง ServiceItemLines เฉพาะชุดที่กำลังดู (เพื่อเอา field ที่ต้องการ)
    const chunks = chunkArray(svSlice, 30);
    let itemLines = [];
    for (const chunk of chunks) {
      const filter = buildDocumentNoFilter(chunk);
      const url = `${base}/ServiceItemLines?${filter}&$select=Document_No,Service_Item_No,Description,USVT_Ref_Sales_Quote_No,USVT_Percent_of_Completion,Repair_Status_Code`;
      const part = await fetchAllOData(url, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      itemLines = itemLines.concat(part);
    }
    const itemByDoc = new Map();
    for (const it of itemLines) {
      if (!itemByDoc.has(it.Document_No)) itemByDoc.set(it.Document_No, []);
      itemByDoc.get(it.Document_No).push(it);
    }

    // mapping ภายใน (u_inspection + u_form)
    const localRows = await new Promise((resolve, reject) => {
      db.query(
        `SELECT i.sv, i.type_form, f.form_name
         FROM u_inspection i
         LEFT JOIN u_form f ON i.type_form = f.id
         WHERE YEAR(i.incoming_date) = ?`,
        [year],
        (err, rows) => (err ? reject(err) : resolve(rows || []))
      );
    });
    const localBySv = new Map(localRows.map(r => [r.sv, { type_form: r.type_form, form_name: r.form_name }]));
    const headerByNo = new Map(headers.map(h => [h.No, h]));

    // รวมแบนคอลัมน์
    const rows = svSlice.map(sv => {
      const h = headerByNo.get(sv) || {};
      const first = (itemByDoc.get(sv) || [])[0] || {};
      const local = localBySv.get(sv) || { type_form: null, form_name: null };
      return {
        sv,
        order_date: h.Order_Date || null,
        job_scope: h.USVT_Job_Scope ?? null,
        ref_sales_quote_no: first.USVT_Ref_Sales_Quote_No ?? null,
        service_item_no: first.Service_Item_No ?? null,
        percent_complete: first.USVT_Percent_of_Completion ?? null,
        repair_status: first.Repair_Status_Code ?? null,
        type_form: local.type_form,
        form_name: local.form_name
      };
    });

    res.json({ page, limit, nextPage, rows });
  } catch (err) {
    console.error('BI /sv-motor summary error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'BI summary error' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// BI 2) ITEM LINES: แตกบรรทัด ServiceItemLines (หลายแถวต่อ SV)
// GET /api/bi/sv-motor-item-lines?year=2025&month=1&page=1&limit=1000
app.get('/api/bi/sv-motor-item-lines', async (req, res) => {
  try {
    const nowYear = new Date().getFullYear();
    const year = Number(req.query.year ?? nowYear);
    const month = req.query.month ? Number(req.query.month) : null;
    const page = Math.max(1, Number(req.query.page ?? 1));
    const limit = Math.min(5000, Math.max(100, Number(req.query.limit ?? 1000)));

    const { startISO, endISO } = startEndISO(year, month);
    const { token, base } = await getBcBaseAndToken();

    const headers = await fetchHeadersByPeriod(base, token, startISO, endISO);
    if (!headers.length) return res.json({ page, limit, nextPage: null, rows: [] });

    const svList = headers.map(h => h.No).filter(Boolean);
    const startIdx = (page - 1) * limit;
    const endIdx = Math.min(svList.length, startIdx + limit);
    const svSlice = svList.slice(startIdx, endIdx);
    const nextPage = endIdx < svList.length ? page + 1 : null;

    const chunks = chunkArray(svSlice, 30);
    let itemLines = [];
    for (const chunk of chunks) {
      const filter = buildDocumentNoFilter(chunk);
      const url = `${base}/ServiceItemLines?${filter}&$select=Document_No,Service_Item_No,Item_No,Description,Quantity,Unit_of_Measure_Code,Repair_Status_Code`;
      const part = await fetchAllOData(url, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      itemLines = itemLines.concat(part);
    }

    // แปลงชื่อคีย์ให้ชัดเจน
    const rows = itemLines.map(it => ({
      sv: it.Document_No,
      service_item_no: it.Service_Item_No ?? null,
      item_no: it.Item_No ?? null,
      description: it.Description ?? null,
      quantity: it.Quantity ?? null,
      uom: it.Unit_of_Measure_Code ?? null,
      repair_status: it.Repair_Status_Code ?? null
    }));

    res.json({ page, limit, nextPage, rows });
  } catch (err) {
    console.error('BI /sv-motor-item-lines error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'BI item-lines error' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// BI 3) ORDER LINES: แตกบรรทัด ServiceOrderLines (หลายแถวต่อ SV)
// GET /api/bi/sv-motor-order-lines?year=2025&month=1&page=1&limit=1000
app.get('/api/bi/sv-motor-order-lines', async (req, res) => {
  try {
    const nowYear = new Date().getFullYear();
    const year = Number(req.query.year ?? nowYear);
    const month = req.query.month ? Number(req.query.month) : null;
    const page = Math.max(1, Number(req.query.page ?? 1));
    const limit = Math.min(5000, Math.max(100, Number(req.query.limit ?? 1000)));

    const { startISO, endISO } = startEndISO(year, month);
    const { token, base } = await getBcBaseAndToken();

    const headers = await fetchHeadersByPeriod(base, token, startISO, endISO);
    if (!headers.length) return res.json({ page, limit, nextPage: null, rows: [] });

    const svList = headers.map(h => h.No).filter(Boolean);
    const startIdx = (page - 1) * limit;
    const endIdx = Math.min(svList.length, startIdx + limit);
    const svSlice = svList.slice(startIdx, endIdx);
    const nextPage = endIdx < svList.length ? page + 1 : null;

    const chunks = chunkArray(svSlice, 30);
    let orderLines = [];
    for (const chunk of chunks) {
      const filter = buildDocumentNoFilter(chunk);
      const url = `${base}/ServiceOrderLines?${filter}&$select=Document_No,ServiceItemNo,USVT_Ref_Sales_Quote_No,USVT_Percent_of_Completion,Type,No,Description,Quantity,Unit_of_Measure_Code`;
      const part = await fetchAllOData(url, {
        Authorization: `Bearer ${token}`,
        Accept: 'application/json'
      });
      orderLines = orderLines.concat(part);
    }

    const rows = orderLines.map(ol => ({
      sv: ol.Document_No,
      service_item_no: ol.ServiceItemNo ?? null,
      ref_sales_quote_no: ol.USVT_Ref_Sales_Quote_No ?? null,
      percent_complete: ol.USVT_Percent_of_Completion ?? null,
      type: ol.Type ?? null,
      no: ol.No ?? null,
      description: ol.Description ?? null,
      quantity: ol.Quantity ?? null,
      uom: ol.Unit_of_Measure_Code ?? null
    }));

    res.json({ page, limit, nextPage, rows });
  } catch (err) {
    console.error('BI /sv-motor-order-lines error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'BI order-lines error' });
  }
});

// Listen
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
