const { QueryTypes } = require("sequelize");
const { db, connectDatabase } = require("./db");
const { setUpExcelLayout } = require("./exceljs-config");

const filename = "export.csv";

const exportExcel = async () => {
  await connectDatabase();
  await db.transaction(async (transaction) => {
    await db.query(
      `
      declare assest_cursor cursor for
      select
          "Tbl_Asset".*,
          "Tbl_Entity"."EntityName",
          "Tbl_Asset_Group"."Asset_Group" as "AssetGroupName",
          "Tbl_Asset_Sub_Group"."Asset_SubGroup_Name" as "AssetSubGroupName",
          "Tbl_Department"."DepartmentName" as "DepartmentName",
          "Tbl_Office_Location"."OfficeName",
          "Tbl_Office_Location"."City",
          "Tbl_Office_Location"."State",
          "Tbl_Office_Location"."Country",
          
          "Tbl_Asset_Transfer_Request"."Request_Id" as "Transfer_Request_Id",	  
          "Tbl_Asset_Transfer_Request_Entity"."EntityName" as "TransferredEntity",
          "Tbl_Asset_Transfer_Request_Department"."DepartmentName" as "TransferredDepartment",
          "Tbl_Asset_Transfer_Request_Office_Location"."OfficeName" as "TransferredOfficeName",
          "Tbl_Asset_Transfer_Request"."Quantity" as "Quantity1",
          "Tbl_Asset_Transfer_Request"."TransferValue" as "TransferValue",
          "Tbl_Asset_Transfer_Request"."DateOfTransfer" as "ApprovalDate1",
          "Tbl_Asset_Transfer_Request"."IGST" as "IGST1",
          "Tbl_Asset_Transfer_Request"."CGST" as "CGST1",
          "Tbl_Asset_Transfer_Request"."SGST" as "SGST1",
          "Tbl_Asset_Transfer_Request"."VAT" as "VAT1",
          "Tbl_Asset_Transfer_Request"."IGSTAmount" as "IGSTAmount1",
          "Tbl_Asset_Transfer_Request"."CGSTAmount" as "CGSTAmount1",
          "Tbl_Asset_Transfer_Request"."SGSTAmount" as "SGSTAmount1",
          "Tbl_Asset_Transfer_Request"."VATAmount" as "VATAmount1",
          "Tbl_Asset_Transfer_Request"."GrossAmount" as "GrossAmount1",
          
          "Tbl_Asset_Sale_Request"."Request_Id" as "Sale_Request_Id",
          "Tbl_Asset_Sale_Request"."VendorName" as "VendorName2",
          "Tbl_Asset_Sale_Request"."Quantity" as "Quantity2",
          "Tbl_Asset_Sale_Request"."SaleValue" as "SaleValue",
          "Tbl_Asset_Sale_Request"."DateOfSale" as "ApprovalDate2",
          "Tbl_Asset_Sale_Request"."IGST" as "IGST2",
          "Tbl_Asset_Sale_Request"."CGST" as "CGST2",
          "Tbl_Asset_Sale_Request"."SGST" as "SGST2",
          "Tbl_Asset_Sale_Request"."VAT" as "VAT2",
          "Tbl_Asset_Sale_Request"."IGSTAmount" as "IGSTAmount2",
          "Tbl_Asset_Sale_Request"."SGSTAmount" as "SGSTAmount2",
          "Tbl_Asset_Sale_Request"."CGSTAmount" as "CGSTAmount2",
          "Tbl_Asset_Sale_Request"."VATAmount" as "VATAmount",
          "Tbl_Asset_Sale_Request"."GrossAmount" as "GrossAmount"
        from "Tbl_Asset"
        left join "Tbl_Asset_Group" on "Tbl_Asset_Group"."Asset_Group_Id" = "Tbl_Asset"."AssetGroup"
        left join "Tbl_Asset_Sub_Group" on "Tbl_Asset_Sub_Group"."Asset_SubGroup_Id" = "Tbl_Asset"."AssetSubGroup"
        left join "Tbl_Department" on "Tbl_Department"."Department_Id" = "Tbl_Asset"."DepartmentId"
        inner join "Tbl_User_Department" on "Tbl_User_Department"."Department_Id" = "Tbl_Department"."Department_Id"
        left join "Tbl_Entity" on "Tbl_Entity"."Entity_Id" = "Tbl_Asset"."EntityId"
        inner join "Tbl_User_Entity" on "Tbl_User_Entity"."Entity_Id" = "Tbl_Entity"."Entity_Id" 
        left join "Tbl_Office_Location" on "Tbl_Office_Location"."Office_Location_Id" = "Tbl_Asset"."OfficeLocationId"
        inner join "Tbl_User_Office_Location" on "Tbl_User_Office_Location"."Office_Location_Id" = "Tbl_Office_Location"."Office_Location_Id" 
        left join "Tbl_Asset_Sale_Request" on "Tbl_Asset_Sale_Request"."Asset_Id" = "Tbl_Asset"."Asset_Id" and "Tbl_Asset_Sale_Request"."Status" = 'APPROVED_AND_CLOSED'
        left join "Tbl_Asset_Transfer_Request" on "Tbl_Asset_Transfer_Request"."Asset_Id" = "Tbl_Asset"."Asset_Id" and "Tbl_Asset_Transfer_Request"."Status" = 'APPROVED_AND_CLOSED'
        left join "Tbl_Entity" as "Tbl_Asset_Transfer_Request_Entity" on "Tbl_Asset_Transfer_Request_Entity"."Entity_Id" = "Tbl_Asset_Transfer_Request"."TransferredEntity"
        left join "Tbl_Department" as "Tbl_Asset_Transfer_Request_Department" on "Tbl_Asset_Transfer_Request"."TransferredDepartment" = "Tbl_Asset_Transfer_Request_Department"."Department_Id"
        left join "Tbl_Office_Location" as "Tbl_Asset_Transfer_Request_Office_Location" on "Tbl_Asset_Transfer_Request_Office_Location"."Office_Location_Id" = "Tbl_Asset_Transfer_Request"."TransferredOfficeName"
        limit 50000;
        `,
      { transaction }
    );
    let { workbook, AssetSheet } = setUpExcelLayout(filename);
    while (true) {
      const result = await db.query("FETCH FORWARD 1000 FROM assest_cursor", {
        type: QueryTypes.SELECT,
        transaction,
      });

      if (!result.length) {
        break;
      }
      result.forEach((row) => {
        AssetSheet.addRow(row).commit();
      });
      console.log(
        result.length,
        `heap usage ${process.memoryUsage().heapUsed / 1024 / 1024}`
      );
    }
    await workbook.commit();

    await db.query("CLOSE assest_cursor", {
      transaction,
    });
  });
};

exportExcel();
