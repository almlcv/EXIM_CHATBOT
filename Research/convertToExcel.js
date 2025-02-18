import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
const Big = require("big.js");

const formatDate = (dateStr) => {
  return dateStr ? new Date(dateStr).toLocaleDateString("en-GB") : "";
};

const formatContainerDates = (containers, dateField) => {
  // Filter out null or empty date strings and map using formatDate
  const validDates = containers
    .map((container) => container[dateField])
    .filter((date) => date) // Removes null and empty strings
    .map((date) => formatDate(date)); // Convert each date to the local format

  if (validDates.length === 0) return ""; // If no valid dates, return empty string

  // Check if all valid dates are the same
  const allDatesSame = validDates.every((date) => date === validDates[0]);
  return allDatesSame ? validDates[0] : validDates.join(",\n");
};

export const convertToExcel = async (
  rows,
  importer,
  status,
  detailedStatus
) => {
  const rowsWithoutBillNo = rows.filter(
    (row) => row.bill_no === "" || row.bill_no === undefined
  );

  if (rowsWithoutBillNo.length === 0) {
    alert("No Data to export");
    return;
  }
  const uniqueDetailedStatuses = [
    ...new Set(
      rowsWithoutBillNo
        .map((row) => row.detailed_status)
        .filter((status) => status !== undefined && status !== null)
    ),
  ];

  const dateOfReport = new Date().toLocaleString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
  });
  const additionalHeaders =
    importer === "BHAVYA MACHINE TOOLS LLP" ||
    importer === "BHAVYA MACHINE TOOLS"
      ? ["HSS NAME"]
      : [];
  const headers = [
    "JOB NO AND DATE",
    ...additionalHeaders,
    "SUPPLIER/ EXPORTER",
    "INVOICE NUMBER AND DATE",
    "INVOICE VALUE AND UNIT PRICE",
    "BL NUMBER AND DATE",
    "COMMODITY",

    "NET WEIGHT",
    "PORT",
    "ARRIVAL DATE",
    "FREE TIME",
    "DETENTION FROM",
    "SHIPPING LINE",
    "CONTAINER NUM & SIZE",
    "WEIGHT EXCESS/SHORTAGE", // New header for weight Excess/Shortage",
    "NUMBER OF CONTAINERS",
    "BE NUMBER AND DATE",
    "REMARKS",
    "DETAILED STATUS",
  ];

  console.log(rowsWithoutBillNo);

  // Row headers
  const dataWithHeaders = rowsWithoutBillNo.map((item) => {
    const jobNoAndDate = `${item.job_no} | ${formatDate(item.job_date)} | ${
      item.custom_house
    } | ${item.type_of_b_e}`;
    const invoiceNoAndDate = `${item.invoice_number} | ${formatDate(
      item.invoice_date
    )}`;
    const blNoAndDate = `${item.awb_bl_no} | ${formatDate(item.awb_bl_date)}`;
    const beNoAndDate = `${item.be_no} | ${formatDate(item.be_date)}`;
    const remarks = `${item.discharge_date ? "Discharge Date: " : "ETA: "}${
      item.discharge_date ? item.discharge_date : item.vessel_berthing
    }${
      item.assessment_date ? ` | Assessment Date: ${item.assessment_date}` : ""
    }
    ${item.rail_out_date ? ` | Rail-Out: ${item.rail_out_date}` : ""}${
      item.examination_date
        ? ` | Examination Date: ${formatDate(item.examination_date)}`
        : ""
    }${
      item.duty_paid_date
        ? ` | Duty Paid Date: ${formatDate(item.duty_paid_date)}`
        : ""
    }${
      item.out_of_charge ? ` | OOC Date: ${formatDate(item.out_of_charge)}` : ""
    }${item.sims_reg_no ? ` | SIMS Reg No: ${item.sims_reg_no}` : ""}${
      item.sims_date ? ` | SIMS Reg Date: ${item.sims_date}` : ""
    }${item.pims_reg_no ? ` | PIMS Reg No: ${item.pims_reg_no}` : ""}${
      item.pims_date ? ` | PIMS Reg Date: ${item.pims_date}` : ""
    }${item.nfmims_reg_no ? ` | NFMIMS Reg No: ${item.nfmims_reg_no}` : ""}${
      item.nfmims_date ? ` | NFMIMS Reg Date: ${item.nfmims_date}` : ""
    }${
      item.obl_telex_bl
        ? ` | ${
            item.obl_telex_bl === "OBL"
              ? `ORG-RCVD: ${item.document_received_date}`
              : `DOC-RCVD: ${item.document_received_date}`
          }`
        : ""
    }${item.do_validity ? ` | DO VALIDITY: ${item.do_validity}` : ""}${
      item.remarks ? ` | Remarks: ${item.remarks}` : ""
    }`;

    const arrivalDates = formatContainerDates(
      item.container_nos,
      "arrival_date"
    );
    const detentionFrom = formatContainerDates(
      item.container_nos,
      "detention_from"
    );

    const containerNumbersWithSizes = item.container_nos
      .map((container) => `${container.container_number} - ${container.size}`)
      .join(",\n");
    const weightExcessShortage = item.container_nos
      .map((container) =>
        container.weight_shortage !== undefined
          ? `${container.weight_shortage}`
          : ""
      )
      .join(",\n");

    const size = item.container_nos
      .map((container) => container.size)
      .join(",\n");
    const cif_amount = new Big(item.cif_amount);
    const exrate = new Big(item.exrate);
    const inv_value = cif_amount.div(exrate).toFixed(2);
    const exact_inv_value = item.total_inv_value
      ? item.total_inv_value.split(" ")[0]
      : "";

    const invoice_value_and_unit_price = `${item.inv_currency} |${exact_inv_value} | ${item.unit_price}`;
    const net_weight = item.container_nos?.reduce((sum, container) => {
      const weight = parseFloat(container.net_weight);
      return sum + (isNaN(weight) ? 0 : weight);
    }, 0);

    const cleanLoadingPort = item.loading_port
      ? item.loading_port.replace(/\(.*?\)\s*/, "")
      : "N/A";
    const cleanPortOfReporting = item.port_of_reporting
      ? item.port_of_reporting.replace(/\(.*?\)\s*/, "")
      : "N/A";

    const valueMap = {
      "JOB NO AND DATE": jobNoAndDate,
      "HSS NAME": item.hss_name,
      "SUPPLIER/ EXPORTER": item.supplier_exporter,
      "INVOICE NUMBER AND DATE": invoiceNoAndDate,
      "INVOICE VALUE AND UNIT PRICE": invoice_value_and_unit_price,
      "BL NUMBER AND DATE": blNoAndDate,
      COMMODITY: item.description,

      "NET WEIGHT": item.job_net_weight,
      PORT: `POL: ${cleanLoadingPort}\nPOD: ${cleanPortOfReporting}`,
      "ARRIVAL DATE": arrivalDates,
      "FREE TIME": item.free_time,
      "DETENTION FROM": detentionFrom,
      "SHIPPING LINE": item.shipping_line_airline,
      "CONTAINER NUM & SIZE": containerNumbersWithSizes,
      "WEIGHT EXCESS/SHORTAGE": weightExcessShortage,
      "NUMBER OF CONTAINERS": item.no_of_container?.slice(0, -2) ?? "",
      "BE NUMBER AND DATE": beNoAndDate,
      REMARKS: remarks,
      "DETAILED STATUS": item.detailed_status,
    };

    // eslint-disable-next-line
    const values = headers.map((val) => {
      if (valueMap[val]) {
        return valueMap[val];
      } else if (val === "CONTAINER NUM & SIZE") {
        return containerNumbersWithSizes;
      } else if (val === "ARRIVAL DATE") {
        return arrivalDates;
      } else if (val === "DETENTION FROM") {
        return detentionFrom;
      } else if (val === "SIZE") {
        return size;
      }
    });

    return values;
  });

  // Create a new workbook
  const workbook = new ExcelJS.Workbook();

  // Add a worksheet
  const worksheet = workbook.addWorksheet("Sheet1");

  ///////////////////////////////////////  Reference Row  //////////////////////////////////////

  const referenceRow = ["REFERENCE", ...uniqueDetailedStatuses];
  worksheet.insertRow(1, referenceRow); // Insert at the top
  // Freeze the first four rows
  worksheet.views = [
    { state: "frozen", ySplit: 4 }, // Freeze the first 4 rows
  ];
  // Apply formatting to the reference row
  const referenceRowExcel = worksheet.getRow(1);
  referenceRowExcel.font = { size: 12, bold: true };
  referenceRowExcel.eachCell({ includeEmpty: true }, (cell, colNumber) => {
    cell.alignment = {
      horizontal: "center",
      vertical: "middle",
      indent: 1, // Adds padding to the left (simulates padding)
      wrapText: true, // Wrap text within the cell
    };
    cell.border = {
      top: { style: "thin" },
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "thin" },
    };

    if (colNumber === 1) {
      // First cell with "REFERENCE"
      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "4472c4" }, // Dark Blue background for REFERENCE
      };
      cell.font = { color: { argb: "FFFFFFFF" }, size: 12, bold: true }; // White text for REFERENCE
    } else {
      // Cells with detailed statuses
      const detailedStatus = cell.value;
      let bgColor = "FFFF99"; // Default color

      // Apply the specific color based on the detailed status
      if (detailedStatus === "ETA Date Pending") {
        bgColor = "ffffffff"; // white
      } else if (detailedStatus === "Estimated Time of Arrival") {
        bgColor = "ffffff99"; // Light Yellow
      } else if (detailedStatus === "Custom Clearance Completed") {
        bgColor = "ffccffff"; // Light Blue
      } else if (detailedStatus === "PCV Done, Duty Payment Pending") {
        bgColor = "ffffdbff"; // Light Blue
      } else if (detailedStatus === "Discharged") {
        bgColor = "ffffcc99"; // Light Orange
      } else if (detailedStatus === "BE Noted, Arrival Pending") {
        bgColor = "ff99ccff"; // Light Purple
      } else if (detailedStatus === "BE Noted, Clearance Pending") {
        bgColor = "ff99ccff"; // Light Purple
      } else if (detailedStatus === "Gateway IGM Filed") {
        bgColor = "ffffcc99"; // Light Orange
      }

      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: bgColor },
      };
      cell.font = { color: { argb: "FF000000" }, size: 12, bold: true }; // Black text for statuses
    }
  });

  ///////////////////////////////////////  Title Row  //////////////////////////////////////
  // Merge cells for the title row
  const endColumnIndex = headers.length - 1;
  const endColumn =
    endColumnIndex < 26
      ? String.fromCharCode(65 + endColumnIndex)
      : String.fromCharCode(64 + Math.floor(endColumnIndex / 26)) +
        String.fromCharCode(65 + (endColumnIndex % 26));
  worksheet.mergeCells(`A3:${endColumn}3`);

  // Set the title for title row
  const titleRow = worksheet.getRow(3);
  titleRow.getCell(1).value = `${importer}: Status as of ${dateOfReport}`;

  // Apply formatting to the title row
  titleRow.font = { size: 12, color: { argb: "FFFFFFFF" } };
  titleRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "4472c4" },
  };
  titleRow.alignment = { horizontal: "center", vertical: "middle" };

  // Set text alignment and borders for the title row
  titleRow.eachCell({ includeEmpty: true }, (cell) => {
    cell.alignment = { horizontal: "center", vertical: "middle" };
    cell.border = {
      top: { style: "thin" },
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "thin" },
    };
  });
  titleRow.height = 40;

  /////////////////////////////////////  Header Row  //////////////////////////////////////
  // Add headers row
  worksheet.addRow(headers);

  // Apply formatting to the header row
  const headerRow = worksheet.getRow(4);
  while (headerRow.cellCount > headers.length) {
    headerRow.getCell(headerRow.cellCount).value = undefined;
  }
  headerRow.font = { size: 12, color: { argb: "FFFFFFFF" } };
  headerRow.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "4472c4" },
  };

  // Set text alignment to center for each cell in the header row
  headerRow.eachCell({ includeEmpty: true }, (cell) => {
    cell.alignment = { horizontal: "center", vertical: "middle" };
    cell.border = {
      top: { style: "thin" },
      left: { style: "thin" },
      bottom: { style: "thin" },
      right: { style: "thin" },
    };
  });

  // Increase the height of the header row
  headerRow.height = 35;

  ///////////////////////////////////////  Data Row  //////////////////////////////////////
  // Add the data rows
  for (const row of dataWithHeaders) {
    const dataRow = worksheet.addRow(row);
    const detailedStatus = row[row.length - 1]; // Get the Detailed Status from the last column

    // Apply background color based on Detailed Status
    if (detailedStatus === "Estimated Time of Arrival") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ffffff99" },
      };
    } else if (detailedStatus === "Custom Clearance Completed") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ffccffff" },
      };
    } else if (detailedStatus === "PCV Done, Duty Payment Pending") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ffffdbff" },
      };
    } else if (detailedStatus === "Discharged") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ffffcc99" },
      };
    } else if (detailedStatus === "BE Noted, Arrival Pending") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ff99ccff" },
      };
    } else if (detailedStatus === "BE Noted, Clearance Pending") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ff99ccff" },
      };
    } else if (detailedStatus === "Gateway IGM Filed") {
      dataRow.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: { argb: "ffffcc99" },
      };
    }

    // Set text alignment to center for each cell in the data row
    dataRow.eachCell({ includeEmpty: true }, (cell) => {
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
        wrapText: true, // Enable text wrapping for all cells
      };

      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" },
      };

      // Add line breaks after commas in the containerNumbersWithSizes  cell
      if (cell.value && cell.value.toString().includes(",\n")) {
        cell.value = cell.value.replace(/,\n/g, String.fromCharCode(10)); // Replace ",\n" with line break character
      }
    });

    // Adjust row height based on content
    const rowHeight = calculateRowHeight(dataRow);

    // Set the calculated row height
    dataRow.height = rowHeight;
  }

  // Function to calculate row height based on content
  function calculateRowHeight(row) {
    let maxHeight = 0;

    row.eachCell({ includeEmpty: true }, (cell) => {
      const lines = cell.value ? cell.value.toString().split(/\r\n|\r|\n/) : [];
      const lineCount = lines.length;
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" },
      };

      // Set a minimum height for the row
      let cellHeight = 100;

      // Calculate the required height for the cell based on the number of lines
      if (lineCount > 1) {
        const defaultFontSize = 12; // Set the default font size used in the cell
        const lineSpacing = 2; // Set the additional spacing between lines

        const totalLineHeight =
          lineCount * defaultFontSize + (lineCount - 1) * lineSpacing;

        // Add padding to the calculated line height
        const padding = 10;

        cellHeight = totalLineHeight + padding;
      }

      // Update the maximum cell height for the row
      if (cellHeight > maxHeight) {
        maxHeight = cellHeight;
      }
    });

    return maxHeight;
  }

  // Adjust column widths based on content
  worksheet.columns.forEach((column, id) => {
    let maxLength = 0;

    column.eachCell({ includeEmpty: true }, (cell) => {
      maxLength = 10;
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" },
      };
    });

    if (headers[id] !== "CONTAINER NUM & SIZE") {
      column.width = maxLength < 25 ? 25 : maxLength;
    }
    if (headers[id] === "JOB NO") {
      column.width = 15;
    }
    if (headers[id] === "BE NUMBER") {
      column.width = 15;
    }
    if (headers[id] === "BE DATE") {
      column.width = 18;
    }
    if (headers[id] === "TYPE OF BE") {
      column.width = 15;
    }
    if (headers[id] === "UNIT") {
      column.width = 12;
    }
    if (headers[id] === "CONTAINER NUM & SIZE") {
      column.width = 30;
    }
    if (headers[id] === "INVOICE VALUE AND UNIT PRICE") {
      column.width = 35;
    }
    if (headers[id] === "IMPORTER") {
      column.width = 40;
    }
    if (headers[id] === "SHIPPING LINE") {
      column.width = 40;
    }
    if (headers[id] === "DESCRIPTION") {
      column.width = 50;
    }
    if (headers[id] === "SIZE") {
      column.width = 10;
    }
    if (headers[id] === "FREE TIME") {
      column.width = 12;
    }
    if (headers[id] === "SUPPLIER/ EXPORTER") {
      column.width = 30;
    }
    if (headers[id] === "STATUS") {
      column.width = 15;
    }
    if (headers[id] === "REMARKS") {
      column.width = 45;
    }
  });

  worksheet.addRow([]);
  worksheet.addRow([]);

  const summaryRow = worksheet.addRow(["SUMMARY", "", "", "", ""]);
  summaryRow.getCell(1).alignment = {
    horizontal: "center",
    vertical: "middle",
  };
  summaryRow.getCell(1).fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "92D050" },
    font: { color: { argb: "FFFFFF" } },
  };

  worksheet.mergeCells(`A${summaryRow.number}:E${summaryRow.number}`); // Merge cells for the "Summary" row

  let containersWithSize20AndArrival = 0;
  let containersWithSize40AndArrival = 0;
  let containersWithSize20AndNoArrival = 0;
  let containersWithSize40AndNoArrival = 0;

  rowsWithoutBillNo.forEach((item) => {
    item.container_nos.forEach((container) => {
      if (container.size === "20" && container.arrival_date) {
        containersWithSize20AndArrival++;
      } else if (container.size === "40" && container.arrival_date) {
        containersWithSize40AndArrival++;
      } else if (container.size === "20" && !container.arrival_date) {
        containersWithSize20AndNoArrival++;
      } else if (container.size === "40" && !container.arrival_date) {
        containersWithSize40AndNoArrival++;
      }
    });
  });

  const totalContainers =
    containersWithSize20AndArrival +
    containersWithSize40AndArrival +
    containersWithSize20AndNoArrival +
    containersWithSize40AndNoArrival;

  // Add the new table with merged cells
  const newTableData = [
    ["20'", "40'", "20'", "40'", ""],
    [
      containersWithSize20AndArrival,
      containersWithSize40AndArrival,
      containersWithSize20AndNoArrival,
      containersWithSize40AndNoArrival,
      totalContainers,
    ],
  ];

  // Get the starting row number for the new table
  const startRow = summaryRow.number + 1; // Adjusted to remove the extra rows

  // Merge cells and apply formatting
  worksheet.addTable({
    name: "SummaryTable",
    ref: `A${startRow}:E${startRow + newTableData.length - 1}`, // Adjusted to match the new data layout
    columns: [
      { name: "ARRIVED" },
      { name: "IN TRANSIT" },
      { name: "TOTAL" },
      { name: "D" },
      { name: "TOTAL" },
    ],
    rows: newTableData,
  });

  for (let row = startRow; row <= startRow + newTableData.length; row++) {
    for (let col = 1; col <= 5; col++) {
      const cell = worksheet.getCell(`${String.fromCharCode(64 + col)}${row}`);
      cell.alignment = {
        horizontal: "center",
        vertical: "middle",
      };
      cell.font = {
        color: "#000000",
      };
      cell.border = {
        top: { style: "thin" },
        left: { style: "thin" },
        bottom: { style: "thin" },
        right: { style: "thin" },
      };
    }
  }

  // Merge cells and add text for "Arrived" and "In Transit"
  worksheet.mergeCells(`A${startRow}:B${startRow}`); // Merge cells for the "Arrived" text
  const arrivedCell = worksheet.getCell(`A${startRow}`);
  arrivedCell.value = "ARRIVED";
  arrivedCell.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "8EAADB" },
  };

  worksheet.mergeCells(`C${startRow}:D${startRow}`); // Merge cells for the "In Transit" text
  const inTransitCell = worksheet.getCell(`C${startRow}`);
  inTransitCell.value = "IN TRANSIT";
  inTransitCell.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "F4B083" },
  };

  const totalCell = worksheet.getCell(`E${startRow}`);
  totalCell.value = "TOTAL";
  totalCell.fill = {
    type: "pattern",
    pattern: "solid",
    fgColor: { argb: "FFFF04" },
  };

  // Apply red background to the first two cells of the last row
  const lastRow = worksheet.getRow(startRow + newTableData.length);
  for (let col = 1; col <= 2; col++) {
    const cell = lastRow.getCell(col);
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "8EAADB" },
    };
  }

  // Apply blue background to the 3rd and 4th cells of the last row
  for (let col = 3; col <= 4; col++) {
    const cell = lastRow.getCell(col);
    cell.fill = {
      type: "pattern",
      pattern: "solid",
      fgColor: { argb: "F4B083" },
    };
  }

  ///////////////////////////////////////////////////////////////////////
  // Generate Excel file
  const excelBuffer = await workbook.xlsx.writeBuffer();

  const data = new Blob([excelBuffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });

  // Sanitize the importer and detailedStatus for the filename
  const sanitizedImporter = importer?.replace(/\./g, "");
  const sanitizedDetailedStatus = detailedStatus?.replace(/\./g, "");

  const newFilename =
    sanitizedDetailedStatus === ""
      ? `${sanitizedImporter} - ${status}.xlsx`
      : `${sanitizedImporter} - ${sanitizedDetailedStatus}.xlsx`;

  saveAs(data, newFilename);
};
