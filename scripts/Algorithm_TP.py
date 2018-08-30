import re,os,hashlib,time,datetime

import sqlite3 as sql
import ROOT as R
import toolkit

'''
entry:
    sample
    TP region
    category
    description:
        nominal -> 1
        sample variation -> 2
        syst variation -> 3
meta info:
    input file
    hash code
    production version
'''

def filter(name):
    #ttbar_1ptag2jet_TP_MLJ_MV2c10_Hyb_PbT60_CalJetPt
    reg = '([^/]*/)?([^/]*)_1ptag2jet_TP_(.*)_(P[xbj][TP])([0-9][0-9])_CalJetPt_?(.*)'
    if not re.match(reg, name):
        return (None,)*6    
    res = re.findall(reg, name)[0]
    sample = res[1]
    category = res[2] + '_' + res[4]
    tp = res[3]
    if res[0] == '' and res[5] == '':
        status = 1
        description = 'Nominal'
        aux = None
    elif res[0]=='Systematics/' and res[5]!='':
        reg_pdf = 'SysPDF4LHC_([0-9]+)'
        reg_rad = 'SysRad(High|Low)'
        reg_syst = 'Sys(.*)__1(down|up)'
        if re.match(reg_pdf,res[5]):
            status = 2
            description = 'PDF4LHC'
            aux = re.findall(reg_pdf,res[5])[0][0]
        elif re.match(reg_rad,res[5]):
            status = 2
            description = 'Radiation'
            aux = re.findall(reg_rad,res[5])[0][0]
        elif re.match(reg_syst,res[5]):
            status = 3
            description,aux = re.findall(reg_syst, res[5])[0]
        else:
            print name,'syst hist not known!'
            raise ValueError()
    else:
        print name,'hist not known!'
        raise ValueError()
    return (sample,tp,category,status,description,aux)






        
        






    

class Root_Histogram_Handler(object):
    def __init__(self,path_to_root_file,version):
        self.version = version
        self.path_to_root_file = path_to_root_file

    def SetFilter(self,fun):
        self.filter = fun

    def SetBinning(self,xbins):
        self.xbins = xbins
        self.str_xbins = '|'.join(map(str,xbins))


    def Digest(self,path_to_data_base,overWrite=False):
        self.DB = sql.connect(path_to_data_base)        
        self._write_meta_data()
        self._write_histograms()
        self.DB.commit()

    def GetMetaData(self,version=None):
        cursor = self.DB.cursor()
        if version!=None:
            cursor.execute('''
SELECT * FROM MetaData
WHERE Version=?
            ''',(version))
        else:
            cursor.execute('''
SELECT * FROM MetaData
            ''')
        itms = cursor.fetchall()
        if len(itms) == 0:
            return None
        else:
            return itms

    def _write_meta_data(self):
        cursor = self.DB.cursor()
        cursor.execute('''
CREATE TABLE IF NOT EXISTS MetaData(
    Version TEXT PRIMARY KEY NOT NULL,    
    Binning TEXT NOT NULL,
    ImportTime TEXT NOT NULL,
    InputFile TEXT NOT NULL,
    HashValue TEXT NOT NULL
);                                                                     
        ''')
        with open(self.path_to_root_file,'rb') as f:
            chunk = f.read()
            hasher = hashlib.md5()
            hasher.update(chunk)
            hash_value = hasher.hexdigest()
        str_time = datetime.utcnow().strftime("%Y-%m-%d,%a,%H:%M:%S UTC+0")
        meta = self.GetMetaData(self.version)
        if meta != None:
            print 'Table exists already!'
            for itm in meta:
                print itm
            raise ValueError()
        cursor.execute('''
INSERT INTO MetaData(Version,Binning,ImportTime,InputFile,HashValue) \
VALUES(?,?,?,?,?);
        ''',(self.version,self.str_xbins,str_time,self.path_to_root_file,hash_value))
        meta = self.GetMetaData(self.version)
        for itm in meta:
            print itm

    def _write_histograms(self):
        self._initialize_table()
        f = R.TFile(self.path_to_root_file,'read')

    def _initialize_table(self):   
        cursor = self.DB.cursor()
        nbins = len(self.xbins)-1
        str_value_error = ''
        keys = 'Sample,TP,Category,Status,Description,Aux'
        for nbin in range(nbins):
            str_value_error += '''
Value_{0:} REAL NOT NULL,
Error_{0:} REAL NOT NULL,
            '''.format(nbin)
            keys += ',Value_{0:},Error_{0:}'.format(nbin)
        self.str_keys = keys
        self.str_question_marks = '?,?,?,?,?'+(',?,?'*nbins)
        cmd = '''
CREATE TABLE Data_%s(
    Sample TEXT NOT NULL,
    TP TEXT NOT NULL,
    Category TEXT NOT NULL,
    Status TEXT NOT NULL,
    Description TEXT NOT NULL,
    Aux TEXT,
    %s
    );
        '''%(self.version,str_value_error[:-2])
        cursor.execute(cmd)

    def _walker(self,root_dir,prefix=''):
        keys = root_dir.GetListOfKeys()
        for key in keys:            
            name = os.path.join(prefix,key.GetName())
            cls_name = key.GetClassName()
            if cls_name == 'TDirectoryFile':
                self._walker(key.ReadObj(),name)
            elif 'TH1' in cls_name:
                self._write_entry(name,key)
            else:
                continue

    def _write_entry(name,key):
        sample,tp,category,status,description,aux = self.filter(name)        
        if status!=None:
            value_errors = self._read_hist(key)
            values = (sample,tp,category,status,description,aux) + value_errors
            cursor = self.DB.cursor()
            cursor.execute('''
SELECT * FROM Data_{version:}
WHERE Sample={sample:} AND TP={tp:} AND Category={category:} AND Status={status:} AND description={description:} AND aux={aux:}
            '''.format(version=self.version,sample=sample,tp=tp,category=category,status=status,description=description,aux=aux))
            entries = cursor.fetchall()
            if len(entries)>=1:
                for entry in entries:
                    print entry
                print name
                raise ValueError("entry already exists!")
            cursor.execute('''
INSERT INTO Data_{version:}({keys:}) \
VALUES({question_marks});
            '''.format(version=self.version,keys=self.str_keys,question_marks=self.str_question_marks),values)
    
    def _read_hist(self,key):
        hist = key.ReadObj()
        values = ()
        for nbin in range(len(self.xbins)-1):
            center = (self.xbins[nbin] + self.xbins[nbin+1])/2.
            nbin_h = hist.GetXaxis().FindBin(center)
            value,error = hist.GetBinContent(nbin_h),hist.GetBinError(nbin_h)
            values += (value,error)
        return values