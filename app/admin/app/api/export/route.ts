/* eslint-disable @typescript-eslint/no-explicit-any, react-hooks/set-state-in-effect */
import { prisma } from "@mono/db";
import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

function toCsv(rows: Record<string, any>[]) {
  if (rows.length === 0) return "";
  const headers = Object.keys(rows[0]);
  const processRow = (row: Record<string, any>) =>
    headers
      .map((header) => {
        let val = row[header];
        if (val === null || val === undefined) val = "";
        val = String(val).replace(/"/g, '""');
        if (val.search(/("|,|\n)/g) >= 0) {
          val = `"${val}"`;
        }
        return val;
      })
      .join(",");

  return [headers.join(","), ...rows.map(processRow)].join("\n");
}

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const type = searchParams.get("type");

  const goldProductId = searchParams.get("goldProductId");
  const goldBrandId = searchParams.get("goldBrandId");
  const goldProductLineId = searchParams.get("goldProductLineId");

  try {
    let data: any[] = [];
    let filename = "export.csv";

    if (type === "goldBrands") {
      const rawRecords = await prisma.goldBrand.findMany({
        orderBy: { mentionCount: "desc" },
      });

      data = rawRecords.map((r) => {
        return { ...r, createdAt: r.createdAt?.toISOString() ?? "", updatedAt: r.updatedAt ? r.updatedAt.toISOString() : undefined };
      });

      filename = "gold-brands.csv";
    } else if (type === "goldLines") {
      const rawRecords = await prisma.goldProductLine.findMany({
        orderBy: { mentionCount: "desc" },
      });

      data = rawRecords.map((r) => {
        return { ...r, createdAt: r.createdAt?.toISOString() ?? "", updatedAt: r.updatedAt ? r.updatedAt.toISOString() : undefined };
      });

      filename = "gold-lines.csv";
    } else if (type === "goldModels") {
      const rawRecords = await prisma.goldProduct.findMany({
        orderBy: { mentionCount: "desc" },
      });

      data = rawRecords.map((r) => {
        return { ...r, createdAt: r.createdAt?.toISOString() ?? "", updatedAt: r.updatedAt ? r.updatedAt.toISOString() : undefined };
      });

      filename = "gold-models.csv";
    } else if (type === "goldTaxonomy") {
      const allBrands = await prisma.goldBrand.findMany({
        orderBy: { mentionCount: "desc" },
        include: {
          productLines: { 
             orderBy: { mentionCount: "desc" },
             include: { goldDepartment: true, categories: true }
          },
          products: { 
             orderBy: { mentionCount: "desc" },
             include: { goldDepartment: true, categories: true }
          }
        }
      });

      for (const brand of allBrands) {
        const _b = {
          Brand_ID: brand.id,
          Brand_Name: brand.canonicalName,
          Brand_Mentions: brand.mentionCount,
          Brand_Sentiment: brand.avgSentiment,
          Brand_CreatedAt: brand.createdAt?.toISOString() ?? "",
        };

        const unallocatedModels = brand.products.filter(p => !p.goldProductLineId);
        
        // Brand has absolutely nothing
        if (brand.productLines.length === 0 && unallocatedModels.length === 0) {
           data.push({
             ..._b,
             Line_ID: "", Line_Name: "", Line_Mentions: "", Line_Sentiment: "", Line_CreatedAt: "", Line_Department: "", Line_Categories: "",
             Model_ID: "", Model_Name: "", Model_Mentions: "", Model_Sentiment: "", Model_CreatedAt: "", Model_Department: "", Model_Categories: ""
           });
        } 
        
        // Iterate Product Lines
        for (const line of brand.productLines) {
           const _l = {
             Line_ID: line.id,
             Line_Name: line.canonicalName,
             Line_Mentions: line.mentionCount,
             Line_Sentiment: line.avgSentiment,
             Line_CreatedAt: line.createdAt?.toISOString() ?? "",
             Line_Department: line.goldDepartment?.canonicalName ?? "",
             Line_Categories: line.categories.map(c => c.canonicalName).join(" | "),
           };

           const lineModels = brand.products.filter(p => p.goldProductLineId === line.id);
           
           if (lineModels.length === 0) {
              data.push({
                ..._b, ..._l,
                Model_ID: "", Model_Name: "", Model_Mentions: "", Model_Sentiment: "", Model_CreatedAt: "", Model_Department: "", Model_Categories: ""
              });
           } else {
              for (const mod of lineModels) {
                 const _m = {
                   Model_ID: mod.id,
                   Model_Name: mod.canonicalName,
                   Model_Mentions: mod.mentionCount,
                   Model_Sentiment: mod.avgSentiment,
                   Model_CreatedAt: mod.createdAt?.toISOString() ?? "",
                   Model_Department: mod.goldDepartment?.canonicalName ?? "",
                   Model_Categories: mod.categories.map(c => c.canonicalName).join(" | "),
                 };
                 data.push({ ..._b, ..._l, ..._m });
              }
           }
        }

        // Iterate Orphaned Models
        for (const mod of unallocatedModels) {
           const _m = {
              Model_ID: mod.id,
              Model_Name: mod.canonicalName,
              Model_Mentions: mod.mentionCount,
              Model_Sentiment: mod.avgSentiment,
              Model_CreatedAt: mod.createdAt?.toISOString() ?? "",
              Model_Department: mod.goldDepartment?.canonicalName ?? "",
              Model_Categories: mod.categories.map(c => c.canonicalName).join(" | "),
           };
           data.push({
              ..._b,
              Line_ID: "", Line_Name: "<ORPHANED>", Line_Mentions: "", Line_Sentiment: "", Line_CreatedAt: "", Line_Department: "", Line_Categories: "",
              ..._m
           });
        }
      }

      filename = "gold-taxonomy.csv";
    } else if (type === "silver") {
      const where = {
        ...(goldProductId ? { goldProductId } : {}),
        ...(goldBrandId ? { goldBrandId } : {}),
        ...(goldProductLineId ? { goldProductLineId } : {}),
      };
      
      const rawRecords = await prisma.silverProductMention.findMany({
        where,
        orderBy: { brand: "asc" },
      });

      data = rawRecords.map((r) => {
         return { ...r };
      });

      filename = "silver-mentions.csv";
      if (goldBrandId) filename = `silver-mentions-brand-${goldBrandId}.csv`;
      if (goldProductLineId) filename = `silver-mentions-line-${goldProductLineId}.csv`;
      if (goldProductId) filename = `silver-mentions-model-${goldProductId}.csv`;
    } else {
      return NextResponse.json({ error: "Invalid type" }, { status: 400 });
    }

    const csvContent = toCsv(data);

    return new NextResponse(csvContent, {
      status: 200,
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${filename}"`,
      },
    });
  } catch (error) {
    console.error("CSV Export Error:", error);
    return NextResponse.json(
      { error: "Failed to generate CSV" },
      { status: 500 }
    );
  }
}
