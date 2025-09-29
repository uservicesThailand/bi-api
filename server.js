// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const db = require('./db');
const axios = require('axios');
const dayjs = require('dayjs');
const compression = require('compression');

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

// เปิด gzip/deflate/br ให้ทุก response ที่เหมาะสม
app.use(compression({
  // ตัวเลือกเสริม (ใช้ค่า default ก็ได้)
  level: 6,                       // 0–9 (6 สมดุลดี)
  threshold: 1024,                // บีบอัดเมื่อใหญ่กว่า 1KB
  filter: (req, res) => {
    // บีบอัดเฉพาะที่ client รับได้ และไม่ใช่ SSE เป็นต้น
    if (req.headers['x-no-compress']) return false;
    return compression.filter(req, res);
  }
}));
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
    /* const headerUrl =
      `${base}/Service_Order_Excel?$select=${headerSelect}&$filter=${encodeURI(headerFilter)}&$orderby=Order_Date desc`; */
    const headerUrl =
      `${base}/Service_Order_Excel?$select=${headerSelect}&$filter=${encodeURI(headerFilter)}&$top=100&$orderby=Order_Date desc`;

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
        `&$select=Document_No,USVT_Ref_Sales_Quote_No,ServiceItemNo,USVT_Percent_of_Completion,Repair_Status_Code`;
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
        SELECT i.sv, i.type_form, f.form_name, ie.elsv_1_1, i.mr_voltage, i.mr_power, i.power_unit,
          p.rpdt_21housingde_1, p.rpdt_22housingnde_1, p.rpdt_23shaftbearde_1
        FROM u_inspection i
        LEFT JOIN u_form f ON i.type_form = f.id
        LEFT JOIN u_inspection_elservice ie ON i.mt_id = ie.mt_id
        LEFT JOIN u_partcheck p ON i.mt_id = p.mt_id
        WHERE YEAR(i.incoming_date) = ?
        `,
        [selectedYear],
        (err, rows) => (err ? reject(err) : resolve(rows || []))
      );
    });
    const localBySv = new Map();

    for (const r of localRows) {
      localBySv.set(r.sv, {
        type_form: r.type_form,
        form_name: r.form_name,
        elsv_1_1: r.elsv_1_1,
        rpdt_21housingde_1: r.rpdt_21housingde_1,
        rpdt_22housingnde_1: r.rpdt_22housingnde_1,
        rpdt_23shaftbearde_1: r.rpdt_23shaftbearde_1,
        mr_voltage: r.mr_voltage,
        mr_power: r.mr_power,
        power_unit: r.power_unit
      });
    }


    // 5) รวมผลลัพธ์ให้ 1 แถวต่อ SV (No)
    const result = headers.map(h => {
      const sv = h.No;

      const itemArr = itemsByDoc.get(sv) || [];
      const orderArr = orderByDoc.get(sv) || [];

      // เอาบรรทัดแรกเป็นตัวแทน (ปรับตามนโยบายได้ภายหลัง)
      const firstItem = itemArr[0] || {};
      const firstOrder = orderArr[0] || {};

      // service item no: เอาจาก itemLines ก่อน ถ้าไม่มีค่อยใช้จาก orderLines
      const service_item_no =
        firstItem.Service_Item_No ||
        firstOrder.ServiceItemNo ||
        "";

      return {
        // คีย์หลักและเมตต้า
        sv, // = Service_Order_Excel.No
        order_date: h.Order_Date || null,
        job_scope: h.USVT_Job_Scope ?? null,

        // จาก ServiceOrderLines (ตาม field ที่มีจริง)
        ref_sales_quote_no: firstOrder.USVT_Ref_Sales_Quote_No ?? null,
        percent_complete: firstOrder.USVT_Percent_of_Completion ?? null,
        Repair_Status_Code: firstOrder.Repair_Status_Code ?? null,

        // จาก ServiceItemLines (ตาม field ที่มีจริง)
        service_item_no,
        item_description: firstItem.Description ?? null,

        // จากระบบภายใน (localRows)
        type_form: (localBySv.get(sv) || {}).type_form ?? null,
        form_name: (localBySv.get(sv) || {}).form_name ?? null,
        elsv_1_1: (localBySv.get(sv) || {}).elsv_1_1 ?? null,
        rpdt_21housingde_1: (localBySv.get(sv) || {}).rpdt_21housingde_1 ?? null,
        rpdt_22housingnde_1: (localBySv.get(sv) || {}).rpdt_22housingnde_1 ?? null,
        rpdt_23shaftbearde_1: (localBySv.get(sv) || {}).rpdt_23shaftbearde_1 ?? null,
        mr_voltage: (localBySv.get(sv) || {}).mr_voltage ?? null,
        mr_power: (localBySv.get(sv) || {}).mr_power ?? null,
        power_unit: (localBySv.get(sv) || {}).power_unit ?? null,

        // แนบรายละเอียดบรรทัด (คงไว้ตามเดิม)
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
// ─── helpers ────────────────────────────────────────────────────────────────
function toIntStrict(val) {
  const s = String(val ?? "").trim();
  if (!s || s.toLowerCase() === "undefined" || s.toLowerCase() === "null") return NaN;
  const n = Number.parseInt(s, 10);
  return Number.isFinite(n) ? n : NaN;
}
function resolveYear(val) {
  const n = toIntStrict(val);
  const y = Number.isFinite(n) && n >= 2000 && n <= 2100 ? n : new Date().getUTCFullYear();
  return y;
}
function resolveMonth(val) {
  const n = toIntStrict(val);
  return Number.isFinite(n) && n >= 1 && n <= 12 ? n : null; // null = ทั้งปี
}
function startEndDateOnly(year, month /* 1..12 or null */) {
  const y = resolveYear(year);
  if (!month) return { startDate: `${y}-01-01`, endDate: `${y}-12-31` };
  const m = String(month).padStart(2, "0");
  // วันสุดท้ายของเดือน (UTC safe)
  const lastDay = new Date(Date.UTC(y, Number(m), 0 /* day 0 of next month */)).getUTCDate();
  return { startDate: `${y}-${m}-01`, endDate: `${y}-${m}-${String(lastDay).padStart(2, "0")}` };
}
function safePage(val) { const n = toIntStrict(val); return Number.isFinite(n) && n >= 1 ? n : 1; }
function safeLimit(val) { const n = toIntStrict(val); return Number.isFinite(n) ? Math.max(100, Math.min(2000, n)) : 1000; }

// ─── handler (เฉพาะช่วงอ่าน query + ทำ URL) ───────────────────────────────
app.get("/api/sv/motor-summary", async (req, res) => {
  try {
    const year = resolveYear(req.query.year);
    const month = resolveMonth(req.query.month); // null = ทั้งปี
    const page = safePage(req.query.page);
    const limit = safeLimit(req.query.limit);
    const skip = (page - 1) * limit;

    // โหมดเร็ว (parallel + chunk ใหญ่) / โหมดเบา (ไม่ดึง lines)
    const fast = String(req.query.fast || "").trim() === "1";
    const lite = String(req.query.lite || "").trim() === "1"; // ยังใช้ได้เหมือนเดิม

    console.log("[sv/motor-summary] y=%s m=%s p=%s lim=%s fast=%s lite=%s",
      year, month ?? "-", page, limit, fast ? "on" : "off", lite ? "on" : "off");

    const token = await getBcAccessToken();
    const company = String(process.env.BC_COMPANY_NAME || "").replace(/'/g, "''");
    const base =
      `https://api.businesscentral.dynamics.com/v2.0/${process.env.BC_TENANT_ID}/${process.env.BC_ENVIRONMENT}` +
      `/ODataV4/Company('${company}')`;

    const { startDate, endDate } = startEndDateOnly(year, month);

    // สำคัญ: Edm.Date → ใช้วันล้วน (ไม่ใส่ T, ไม่ใส่ datetimeoffset)
    const headerSelect = ["No", "Order_Date", "USVT_Job_Scope"].join(",");
    const headerFilterRaw = `(Order_Date ge ${startDate} and Order_Date le ${endDate})`;
    const headerUrl =
      `${base}/Service_Order_Excel?$select=${headerSelect}` +
      `&$filter=${encodeURIComponent(headerFilterRaw)}` +
      `&$orderby=Order_Date desc,No asc` +
      `&$skip=${skip}&$top=${limit}`;

    const headersResp = await axios.get(headerUrl, {
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      timeout: 120000 // กันเน็ตหน่วง
    });
    const headers = Array.isArray(headersResp.data?.value) ? headersResp.data.value : [];
    if (!headers.length) {
      res.setHeader('X-Page', String(page));
      res.setHeader('X-Limit', String(limit));
      res.setHeader('X-Fast', fast ? "1" : "0");
      res.setHeader('X-Lite', lite ? "1" : "0");
      return res.json([]);
    }

    // ลิสต์ SV ในเพจนี้
    const svList = headers.map(h => h.No).filter(Boolean);

    // ── local DB (โหลดเฉพาะ sv ในเพจ) ─────────────────────────────────────
    const bindSv = svList.map(_ => '?').join(',');
    const localRows = await new Promise((resolve, reject) => {
      if (!svList.length) return resolve([]);
      db.query(
        `
          SELECT i.sv, i.type_form, f.form_name, ie.elsv_1_1, i.mr_voltage, i.mr_power, i.power_unit,
                 p.rpdt_21housingde_1, p.rpdt_22housingnde_1, p.rpdt_23shaftbearde_1
          FROM u_inspection i
          LEFT JOIN u_form f ON i.type_form = f.form_type
          LEFT JOIN u_inspection_elservice ie ON i.mt_id = ie.mt_id
          LEFT JOIN u_partcheck p ON i.mt_id = p.mt_id
          WHERE i.sv IN (${bindSv})
        `,
        svList,
        (err, rows) => (err ? reject(err) : resolve(rows || []))
      );
    });
    const localBySv = new Map();
    for (const r of localRows) {
      localBySv.set(r.sv, {
        type_form: r.type_form,
        form_name: r.form_name,
        elsv_1_1: r.elsv_1_1,
        rpdt_21housingde_1: r.rpdt_21housingde_1,
        rpdt_22housingnde_1: r.rpdt_22housingnde_1,
        rpdt_23shaftbearde_1: r.rpdt_23shaftbearde_1,
        mr_voltage: r.mr_voltage,
        mr_power: r.mr_power,
        power_unit: r.power_unit,
      });
    }

    // ── Lines: ทำเฉพาะถ้าไม่ใช่ lite ───────────────────────────────────────
    let itemsByDoc = new Map();
    let orderByDoc = new Map();

    if (!lite) {
      // ปรับขนาด chunk และยิงขนาน
      const CHUNK = fast ? 120 : 30;

      const chunks = (arr, n) => {
        const out = [];
        for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
        return out;
      };
      const buildDocumentNoFilter = (arr) =>
        `$filter=${arr.map(v => `Document_No eq '${String(v).replace(/'/g, "''")}'`).join(' or ')}`;

      const svChunks = chunks(svList, CHUNK);

      // สร้าง promises ยิงขนานสำหรับ ItemLines และ OrderLines
      const itemPromises = svChunks.map(chunk =>
        axios.get(
          `${base}/ServiceItemLines?${buildDocumentNoFilter(chunk)}&$select=Document_No,Service_Item_No,Description`,
          { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }, timeout: 120000 }
        ).then(r => r.data?.value || [])
      );

      const orderPromises = svChunks.map(chunk =>
        axios.get(
          `${base}/ServiceOrderLines?${buildDocumentNoFilter(chunk)}&$select=Document_No,USVT_Ref_Sales_Quote_No,ServiceItemNo,USVT_Percent_of_Completion,Repair_Status_Code`,
          { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }, timeout: 120000 }
        ).then(r => r.data?.value || [])
      );

      // ยิงขนานสองชุดพร้อมกัน
      const [itemParts, orderParts] = await Promise.all([
        Promise.all(itemPromises),
        Promise.all(orderPromises),
      ]);

      // รวมผล
      const itemLines = itemParts.flat();
      const orderLines = orderParts.flat();

      for (const it of itemLines) {
        const k = it.Document_No;
        if (!itemsByDoc.has(k)) itemsByDoc.set(k, []);
        itemsByDoc.get(k).push(it);
      }
      for (const ol of orderLines) {
        const k = ol.Document_No;
        if (!orderByDoc.has(k)) orderByDoc.set(k, []);
        orderByDoc.get(k).push(ol);
      }
    }

    // ── รวมผลลัพธ์แบบ "เส้นเดียว" ────────────────────────────────────────
    const result = headers.map(h => {
      const sv = h.No;
      const itemArr = itemsByDoc.get(sv) || [];
      const orderArr = orderByDoc.get(sv) || [];

      const firstItem = itemArr[0] || {};
      const firstOrder = orderArr[0] || {};
      const service_item_no = firstItem.Service_Item_No || firstOrder.ServiceItemNo || '';
      const local = localBySv.get(sv) || {};

      return {
        sv,
        order_date: h.Order_Date || null,
        job_scope: h.USVT_Job_Scope ?? null,

        ref_sales_quote_no: firstOrder.USVT_Ref_Sales_Quote_No ?? null,
        percent_complete: firstOrder.USVT_Percent_of_Completion ?? null,
        Repair_Status_Code: firstOrder.Repair_Status_Code ?? null,

        service_item_no,
        item_description: firstItem.Description ?? null,

        type_form: local.type_form ?? null,
        form_name: local.form_name ?? null,
        elsv_1_1: local.elsv_1_1 ?? null,
        rpdt_21housingde_1: local.rpdt_21housingde_1 ?? null,
        rpdt_22housingnde_1: local.rpdt_22housingnde_1 ?? null,
        rpdt_23shaftbearde_1: local.rpdt_23shaftbearde_1 ?? null,
        mr_voltage: local.mr_voltage ?? null,
        mr_power: local.mr_power ?? null,
        power_unit: local.power_unit ?? null,
      };
    });

    res.setHeader('X-Page', String(page));
    res.setHeader('X-Limit', String(limit));
    res.setHeader('X-Fast', fast ? "1" : "0");
    res.setHeader('X-Lite', lite ? "1" : "0");
    res.json(result);
  } catch (err) {
    console.error('SV MOTOR SUMMARY Error:', err?.response?.data || err?.message || err);
    res.status(500).json({ error: 'เกิดข้อผิดพลาดในการดึง motor-summary' });
  }
});

// เพิ่มก่อน app.listen
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Power BI API Server',
    version: '1.0.0',
    timestamp: new Date().toISOString()
  });
});

// Routes
app.get('/api/bc/data', bcDataHandler);
app.post('/api/bc/data', bcDataHandler);

app.get('/api/sv/motor', svMotorHandler);
app.post('/api/sv/motor', svMotorHandler);

// Listen
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});