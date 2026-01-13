const runBtn = document.getElementById("run-query");
const sqlInput = document.getElementById("sql");
const resultContainer = document.getElementById("result-container");
const tableList = document.getElementById("table-list");
const queryHistory = document.getElementById("query-history");

const createTableBtn = document.getElementById("create-table");
const dropTableBtn = document.getElementById("drop-table");
const newTableNameInput = document.getElementById("new-table-name");

refreshTables();
// ----- Drop Table -----
const dropModal = document.getElementById("drop-table-modal");
const dropTableNameSpan = document.getElementById("drop-table-name");
const confirmDropBtn = document.getElementById("confirm-drop-table");
const cancelDropBtn = document.getElementById("cancel-drop-table");

let history = [];

// Modal Elements
const modal = document.getElementById("create-table-modal");
const modalClose = modal.querySelector(".close");
const addColBtn = document.getElementById("add-column");
const modalCreateBtn = document.getElementById("modal-create-table");
const columnsContainer = document.getElementById("columns-container");
const modalTableName = document.getElementById("modal-table-name");

// Open modal on create table button
createTableBtn.addEventListener("click", () => {
    modal.style.display = "block";
});

// Close modal
modalClose.addEventListener("click", () => modal.style.display = "none");
window.addEventListener("click", (e) => {
    if (e.target === modal) modal.style.display = "none";
});

// Add new column row
addColBtn.addEventListener("click", () => {
    const row = document.createElement("div");
    row.className = "column-row";
    row.innerHTML = `
        <input type="text" placeholder="Column Name" class="col-name">
        <select class="col-type">
            <option value="INT">INT</option>
            <option value="TEXT">TEXT</option>
            <option value="BOOL">BOOL</option>
        </select>
        <label><input type="checkbox" class="col-primary"> PK</label>
        <label><input type="checkbox" class="col-unique"> Unique</label>
        <button class="remove-col">Remove</button>
    `;
    columnsContainer.appendChild(row);

    row.querySelector(".remove-col").addEventListener("click", () => row.remove());
});

// Handle create table from modal
modalCreateBtn.addEventListener("click", () => {
    const tableName = modalTableName.value.trim();
    if (!tableName) return alert("Table name required");

    const columns = {};
    const uniqueColumns = [];
    let primaryKey = null;

    const rows = columnsContainer.querySelectorAll(".column-row");
    for (const r of rows) {
        const colName = r.querySelector(".col-name").value.trim();
        const colType = r.querySelector(".col-type").value;
        const isPK = r.querySelector(".col-primary").checked;
        const isUnique = r.querySelector(".col-unique").checked;

        if (!colName) return alert("Column name cannot be empty");

        columns[colName] = colType;

        if (isPK) primaryKey = colName;
        if (isUnique) uniqueColumns.push(colName);
    }

    if (!primaryKey) return alert("Please select a primary key");

    // Build CREATE TABLE SQL
    const colsDef = Object.entries(columns).map(([name, type]) => {
        const pk = name === primaryKey ? "PRIMARY" : "";
        const uq = uniqueColumns.includes(name) ? "UNIQUE" : "";
        return `${name} ${type} ${pk} ${uq}`.trim();
    }).join(", ");

    const sql = `CREATE TABLE ${tableName} (${colsDef});`;
    sqlInput.value = sql;
    runBtn.click();

    // Close modal and reset
    modal.style.display = "none";
    modalTableName.value = "";
    columnsContainer.innerHTML = "";
    addColBtn.click(); // add default first row
});


// ---------------- Query execution ----------------
runBtn.addEventListener("click", async () => {
    const sql = sqlInput.value.trim();
    if (!sql) return;

    try {
        const response = await fetch("/query", {
            method: "POST",
            headers: { "Content-Type": "application/x-www-form-urlencoded" },
            body: `sql=${encodeURIComponent(sql)}`
        });

        const data = await response.json();
        if (data.error) {
            resultContainer.innerHTML = `<span class="error">${data.error}</span>`;
        } else {
            if (Array.isArray(data.result)) {
                resultContainer.innerHTML = formatTable(data.result);
            } else {
                resultContainer.textContent = data.result;
            }
            addToHistory(sql);
            await refreshTables();
        }
    } catch (err) {
        resultContainer.innerHTML = `<span class="error">${err}</span>`;
    }
});

// ---------------- Table management ----------------
createTableBtn.addEventListener("click", async () => {
    const tableName = newTableNameInput.value.trim();
    if (!tableName) return;

    const sql = `CREATE TABLE ${tableName} (id INT PRIMARY);`; 
    sqlInput.value = sql;
    runBtn.click();
});

dropTableBtn.addEventListener("click", async () => {
    const tableName = newTableNameInput.value.trim();
    if (!tableName) return;

    const sql = `DROP TABLE ${tableName};`;
    sqlInput.value = sql;
    runBtn.click();
});

// ---------------- Query history ----------------
function addToHistory(query) {
    history.unshift(query);
    renderHistory();
}

function renderHistory() {
    queryHistory.innerHTML = "";
    history.slice(0, 10).forEach(q => {
        const li = document.createElement("li");
        li.textContent = q;
        li.addEventListener("click", () => {
            sqlInput.value = q;
        });
        queryHistory.appendChild(li);
    });
}

// ---------------- Table list ----------------
async function refreshTables() {
    try {
        const response = await fetch("/tables");
        const data = await response.json();
        tableList.innerHTML = "";
        data.tables.forEach(table => {
            const li = document.createElement("li");
            li.textContent = table;
            li.addEventListener("click", () => {
                sqlInput.value = `SELECT * FROM ${table};`;
            });
            tableList.appendChild(li);
        });
    } catch (err) {
        console.error("Error fetching tables", err);
    }
}

// ---------------- Helper ----------------
function formatTable(rows) {
    if (rows.length === 0) return "No rows found.";
    let table = "<table><thead><tr>";
    Object.keys(rows[0]).forEach(col => { table += `<th>${col}</th>`; });
    table += "</tr></thead><tbody>";
    rows.forEach(row => {
        table += "<tr>";
        Object.values(row).forEach(val => { table += `<td>${val}</td>`; });
        table += "</tr>";
    });
    table += "</tbody></table>";
    return table;
}

// Initial fetch of tables


// Open drop modal on table delete button click
document.addEventListener("click", (e) => {
    if (e.target.classList.contains("drop-table-btn")) {
        const tableName = e.target.dataset.table;
        dropTableNameSpan.textContent = tableName;
        dropModal.style.display = "block";
        confirmDropBtn.dataset.table = tableName;
    }
});

// Close modal
dropModal.querySelector(".close").addEventListener("click", () => dropModal.style.display = "none");
cancelDropBtn.addEventListener("click", () => dropModal.style.display = "none");

// Confirm drop
confirmDropBtn.addEventListener("click", async () => {
    const tableName = confirmDropBtn.dataset.table;
    sqlInput.value = `DROP TABLE ${tableName};`;
    runBtn.click();
    dropModal.style.display = "none";
});

// ----- Edit Row -----
const editModal = document.getElementById("edit-row-modal");
const editContainer = document.getElementById("edit-row-container");
const saveRowBtn = document.getElementById("save-row-changes");
let currentRowData = null;
let currentTable = null;

// Open edit modal
document.addEventListener("click", (e) => {
    if (e.target.classList.contains("edit-row-btn")) {
        currentTable = e.target.dataset.table;
        currentRowData = JSON.parse(e.target.dataset.row);

        editContainer.innerHTML = "";
        for (const [col, val] of Object.entries(currentRowData)) {
            const input = document.createElement("input");
            input.value = val;
            input.dataset.col = col;
            editContainer.appendChild(input);
        }
        editModal.style.display = "block";
    }
});

// Close modal
editModal.querySelector(".close").addEventListener("click", () => editModal.style.display = "none");

// Save changes
saveRowBtn.addEventListener("click", () => {
    const updates = {};
    editContainer.querySelectorAll("input").forEach(inp => {
        updates[inp.dataset.col] = inp.value;
    });

    // Build UPDATE query
    const whereClause = Object.entries(currentRowData)
        .map(([col, val]) => `${col} = '${val}'`)
        .join(" AND ");
    const setClause = Object.entries(updates)
        .map(([col, val]) => `${col} = '${val}'`)
        .join(", ");

    sqlInput.value = `UPDATE ${currentTable} SET ${setClause} WHERE ${whereClause};`;
    runBtn.click();
    editModal.style.display = "none";
});
