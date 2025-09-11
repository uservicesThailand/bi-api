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

// Listen
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
