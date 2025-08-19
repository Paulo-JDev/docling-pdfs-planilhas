from docling.document_converter import DocumentConverter

from pathlib import Path



print("🔄 Iniciando conversão...")



converter = DocumentConverter()

pdf_local = Path(__file__).parent / "base" / "1- Anexo-I-Termo-de-Referencia---PE-90031-2024.pdf-print.pdf"



print(f"📂 Lendo PDF: {pdf_local}")

result_local = converter.convert(str(pdf_local))



print("✅ Conversão concluída! Salvando em result.md...")



with open("result.md", "w", encoding="utf-8") as f:
    f.write(result_local.document.export_to_markdown())
print("🎉 Finalizado! Arquivo salvo como result.md")
