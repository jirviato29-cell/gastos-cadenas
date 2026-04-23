import re, os

TDIR = r'C:\Users\Administrador\Desktop\gastos-cadenas\templates'

HEAD_LINKS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">\n'
    '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">\n'
    '<link rel="stylesheet" href="{{ url_for(\'static\', filename=\'style.css\') }}">'
)

PAGE_CSS = {}

PAGE_CSS['gastos.html'] = """
.main{padding:16px;max-width:1100px;margin:0 auto;}
.controls{display:flex;align-items:flex-end;gap:10px;flex-wrap:wrap;margin-bottom:14px;background:#fff;border:1px solid #E8EDF5;border-radius:12px;padding:14px;box-shadow:0 2px 8px rgba(0,28,75,.06);}
.controls label{font-size:11px;color:#5A6A80;margin-bottom:4px;font-weight:600;text-transform:uppercase;display:block;}
.controls select{min-width:280px;}
.btn-verde{background:#16A34A;color:#fff;}
.btn:disabled{background:#C5D0E0;cursor:not-allowed;opacity:1;}
.tabs-wrap{background:#fff;border:1px solid #E8EDF5;border-radius:12px 12px 0 0;padding:12px 14px 0;margin-bottom:0;}
.tabs{display:flex;flex-wrap:wrap;gap:5px;}
.tab{padding:7px 15px;border-radius:7px 7px 0 0;border:1.5px solid #D8E2F0;border-bottom:none;font-size:12px;font-weight:600;cursor:pointer;background:#F5F7FA;color:#64748B;transition:all .15s;font-family:'Inter',sans-serif;}
.tab:hover{border-color:#002855;color:#002855;background:#fff;}
.tab.active{background:#002855;color:#fff;border-color:#002855;}
.tab-body{background:#fff;border:1.5px solid #E8EDF5;border-top:3px solid #FF6B00;border-radius:0 0 12px 12px;margin-bottom:14px;overflow:hidden;}
th{text-align:left;}
th:last-child{text-align:right;}
td:last-child{text-align:right;font-variant-numeric:tabular-nums;font-weight:600;}
td:first-child{font-weight:600;color:#002855;}
tfoot td:last-child{font-size:14px;}
.frow label{display:block;font-size:11px;color:#5A6A80;margin-bottom:4px;font-weight:600;text-transform:uppercase;}
.frow input,.frow select{width:100%;padding:9px 12px;border:1.5px solid #D8E2F0;border-radius:8px;font-size:13px;}
.preview-sem{background:#EEF2F7;border-radius:8px;padding:10px 12px;font-size:13px;font-weight:700;color:#002855;margin-top:8px;min-height:36px;}
@media print{.controls,.tabs-wrap{display:none!important;}.tab-body{border-top:none!important;border-radius:0!important;}}
"""

PAGE_CSS['balance.html'] = """
.main{padding:24px;max-width:860px;margin:0 auto;}
.controls{display:flex;gap:12px;align-items:flex-end;margin-bottom:20px;flex-wrap:wrap;}
.controls>div{display:flex;flex-direction:column;gap:4px;}
select{min-width:300px;}
.estado{background:#fff;border:1px solid #E8EDF5;border-radius:14px;padding:40px 48px;box-shadow:0 4px 24px rgba(0,28,75,.09);}
.estado-header{text-align:center;margin-bottom:32px;padding-bottom:20px;border-bottom:2px solid #002855;}
.estado-header .empresa{font-size:11px;letter-spacing:2px;text-transform:uppercase;color:#94A3B8;margin-bottom:6px;}
.estado-header .titulo{font-size:24px;font-weight:800;color:#002855;}
.estado-header .periodo{font-size:13px;color:#64748B;margin-top:6px;font-weight:500;}
.seccion{margin-bottom:28px;}
.sec-title{font-size:14px;font-weight:800;letter-spacing:3px;text-transform:uppercase;color:#002855;margin-bottom:14px;padding-bottom:7px;border-bottom:3px solid #FF6B00;display:inline-block;}
.linea-sep{border:none;border-top:1px solid #E8EDF5;margin:8px 0;}
.fila{display:flex;align-items:baseline;padding:6px 0;font-size:13.5px;}
.fila-sub{display:flex;align-items:baseline;padding:4px 0 4px 24px;font-size:13px;}
.fila-sub .concepto{color:#64748B;}
.concepto{flex:1;color:#1A202C;}
.detalle{font-size:11px;color:#94A3B8;margin-left:6px;font-weight:400;white-space:nowrap;}
.monto{font-variant-numeric:tabular-nums;font-weight:700;white-space:nowrap;min-width:140px;text-align:right;color:#002855;}
.monto.rojo{color:#DC2626;}
.monto.verde{color:#16A34A;}
.fila-total{display:flex;align-items:baseline;padding:12px 0 8px;font-size:15px;font-weight:700;border-top:1.5px solid #CBD5E1;}
.fila-total .concepto{color:#002855;}
.fila-total .monto{font-size:16px;}
.resumen{background:linear-gradient(135deg,#002855 0%,#003D80 100%);border-radius:12px;padding:28px 36px;margin-top:4px;}
.resumen .sec-title{color:rgba(255,255,255,.9);border-bottom-color:rgba(255,107,0,.6);}
.res-fila{display:flex;align-items:baseline;padding:8px 0;font-size:14px;}
.res-fila .concepto{flex:1;color:rgba(255,255,255,.8);font-weight:500;}
.res-fila .monto{font-variant-numeric:tabular-nums;font-weight:700;min-width:160px;text-align:right;font-size:15px;color:#fff;}
.res-sep{border:none;border-top:1px solid rgba(255,255,255,.18);margin:4px 0;}
.res-doble{border:none;border-top:2px solid rgba(255,255,255,.35);margin:8px 0;}
.utilidad-fila{display:flex;align-items:baseline;padding:14px 0 6px;}
.utilidad-fila .concepto{flex:1;font-size:17px;font-weight:700;color:#fff;}
.utilidad-fila .monto{font-variant-numeric:tabular-nums;font-weight:800;font-size:28px;min-width:160px;text-align:right;}
.margen-fila{display:flex;padding:4px 0;font-size:12px;color:rgba(255,255,255,.55);}
.margen-fila span{margin-left:auto;font-weight:700;font-size:14px;color:rgba(255,255,255,.85);}
.resumen .monto.verde{color:#4ADE80;}
.resumen .monto.rojo{color:#FC8181;}
@media print{.estado{border:none!important;box-shadow:none!important;padding:0!important;}.resumen{-webkit-print-color-adjust:exact;print-color-adjust:exact;}}
"""

PAGE_CSS['tiendas.html'] = """
.main{padding:16px;max-width:1100px;margin:0 auto;}
th:nth-child(4),th:nth-child(5){text-align:left;}
td:nth-child(4),td:nth-child(5){text-align:left;}
td:first-child{font-weight:700;color:#002855;}
.sub-row{display:flex;gap:6px;margin-bottom:5px;}
.sub-row input{flex:1;}
.sub-row button{background:none;border:1px solid #D0D9E8;border-radius:6px;padding:5px 9px;cursor:pointer;color:#888;font-size:13px;}
.conf{font-size:12px;font-weight:700;color:#16A34A;padding:4px 0;display:none;}
.frow label{display:block;font-size:11px;color:#5A6A80;margin-bottom:4px;font-weight:600;text-transform:uppercase;}
.frow input,.frow select{width:100%;padding:9px 12px;border:1.5px solid #D8E2F0;border-radius:8px;font-size:13px;}
.modal{width:480px;}
"""

PAGE_CSS['telcel.html'] = """
.main{padding:16px;max-width:900px;margin:0 auto;}
select{width:auto;}
.drop-zone{border:2.5px dashed #A3BBDF;border-radius:10px;padding:36px;text-align:center;color:#94A3B8;cursor:pointer;transition:border-color .2s,background .2s;margin:12px 0;}
.drop-zone:hover,.drop-zone.over{border-color:#FF6B00;background:#FFF8F5;}
.drop-zone input[type=file]{display:none;}
.drop-zone .icon{font-size:36px;margin-bottom:8px;}
.drop-zone .sub{font-size:12px;margin-top:4px;}
.result-box{border-radius:10px;padding:14px;margin-top:12px;display:none;}
.result-ok{background:#DCFCE7;border:1.5px solid #16A34A;color:#14532D;}
.result-err{background:#FEE2E2;border:1.5px solid #F87171;color:#7F1D1D;}
.prog{display:none;text-align:center;padding:16px;color:#002855;font-size:13px;}
th:last-child{text-align:right;}
td:last-child{text-align:right;font-weight:600;font-variant-numeric:tabular-nums;}
tfoot td{background:#EEF2F7;color:#1A202C;font-weight:700;border-top:2px solid #002855;}
"""

PAGE_CSS['resumen.html'] = """
.main{padding:16px;max-width:1000px;margin:0 auto;}
select{width:auto;min-width:280px;}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px;}
.stat{background:#fff;border:1px solid #E8EDF5;border-radius:10px;padding:16px;text-align:center;box-shadow:0 2px 8px rgba(0,28,75,.06);}
.stat .n{font-size:22px;font-weight:700;}
.stat .l{font-size:11px;color:#94A3B8;margin-top:4px;}
th:nth-child(4),th:nth-child(5),th:nth-child(6),th:nth-child(7){text-align:right;}
td:nth-child(3){text-align:center;}
td:nth-child(4),td:nth-child(5),td:nth-child(6),td:nth-child(7){text-align:right;font-variant-numeric:tabular-nums;}
tfoot td:nth-child(4),tfoot td:nth-child(5),tfoot td:nth-child(6),tfoot td:nth-child(7){text-align:right;}
.tr-cadena td{background:#EEF2F7;font-weight:700;}
.pos{color:#16A34A;font-weight:700;}
.neg{color:#DC2626;font-weight:700;}
"""

PAGE_CSS['comisiones.html'] = """
.main{padding:16px;max-width:1200px;margin:0 auto;}
select{width:auto;}
.filtros{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;align-items:flex-end;}
.filtros>div{display:flex;flex-direction:column;gap:4px;}
.filtros select{min-width:180px;}
th{text-align:right;}
th:first-child,th:nth-child(2),th:nth-child(3){text-align:left;}
td{text-align:right;white-space:nowrap;}
td:first-child,td:nth-child(2),td:nth-child(3){text-align:left;}
tfoot td{text-align:right;}
.sin-tienda{color:#DC2626;font-size:11px;}
.stat-row{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;}
.stat-card{flex:1;min-width:90px;background:#F5F7FA;border:1px solid #E8EDF5;border-radius:10px;padding:12px 14px;text-align:center;box-shadow:0 1px 4px rgba(0,28,75,.05);}
.stat-card .l{font-size:10px;color:#94A3B8;font-weight:600;text-transform:uppercase;letter-spacing:.3px;margin-bottom:6px;}
.stat-card .n{font-size:18px;font-weight:700;color:#002855;}
.stat-card.total{background:#002855;border-color:#002855;}
.stat-card.total .l{color:rgba(255,255,255,.65);}
.stat-card.total .n{font-size:22px;color:#fff;}
.stat-card.sin .n{color:#DC2626;}
"""

PAGE_CSS['comisiones_extra.html'] = """
.main{padding:16px;max-width:900px;margin:0 auto;}
select{width:auto;min-width:280px;}
th:last-child{text-align:right;}
td:last-child{text-align:right;font-variant-numeric:tabular-nums;font-weight:600;}
tfoot td:last-child{text-align:right;}
.frow label{display:block;font-size:11px;color:#5A6A80;margin-bottom:4px;font-weight:600;text-transform:uppercase;}
.frow input,.frow select{width:100%;padding:9px 12px;border:1.5px solid #D8E2F0;border-radius:8px;font-size:13px;}
.b1{background:#DBEAFE;color:#1E40AF;}
.b2{background:#DCFCE7;color:#166534;}
.b3{background:#FEF9C3;color:#92400E;}
.b4{background:#F3E8FF;color:#6B21A8;}
"""

PAGE_CSS['promotores.html'] = """
.main{padding:16px;max-width:1200px;margin:0 auto;}
input,select{width:100%;padding:9px 12px;border:1.5px solid #D8E2F0;border-radius:8px;font-size:13px;}
.search{width:260px;margin-bottom:10px;}
th{text-align:center;}
th:first-child{text-align:left;}
td{text-align:center;border-bottom:1px solid #EEF2F9;}
td:first-child{text-align:left;font-weight:600;color:#002855;}
.badge-verde{background:#DCFCE7;color:#166534;}
.badge-rojo{background:#FEE2E2;color:#991B1B;}
.badge-azul{background:#DBEAFE;color:#1E40AF;}
.frow2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px;}
.frow label{display:block;font-size:11px;color:#5A6A80;margin-bottom:4px;font-weight:600;text-transform:uppercase;}
.frow input,.frow select{width:100%;padding:9px 12px;border:1.5px solid #D8E2F0;border-radius:8px;font-size:13px;}
.check-row{display:flex;align-items:center;gap:8px;margin-top:6px;}
.check-row input[type=checkbox]{width:auto;accent-color:#FF6B00;}
.conf{font-size:12px;font-weight:700;color:#16A34A;padding:4px 0;display:none;}
.drop-zone{border:2.5px dashed #A3BBDF;border-radius:10px;padding:28px;text-align:center;color:#94A3B8;cursor:pointer;transition:border-color .2s,background .2s;margin:12px 0;}
.drop-zone:hover,.drop-zone.over{border-color:#FF6B00;background:#FFF8F5;}
.drop-zone input[type=file]{display:none;}
.result-imp{border-radius:8px;padding:12px;margin-top:10px;font-size:12px;display:none;}
.result-ok{background:#DCFCE7;border:1.5px solid #16A34A;color:#14532D;}
.result-err{background:#FEE2E2;border:1.5px solid #F87171;color:#7F1D1D;}
.modal{width:480px;max-height:90vh;}
"""


def process(fname, page_css):
    path = os.path.join(TDIR, fname)
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Replace entire <style>...</style> block with page-specific CSS only
    new_style = '<style>\n' + page_css.strip() + '\n</style>'
    content = re.sub(r'<style>.*?</style>', new_style, content, flags=re.DOTALL)

    # Insert Google Fonts + shared CSS link before first <style> tag
    content = content.replace('<style>', HEAD_LINKS + '\n<style>', 1)

    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print('OK  ' + fname)


for fname, css in PAGE_CSS.items():
    try:
        process(fname, css)
    except Exception as e:
        print('ERR ' + fname + ': ' + str(e))
