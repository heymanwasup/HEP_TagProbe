import re,os,hashlib,time,datetime,functools

import sqlite3 as sql
import ROOT as R

def TimeCounter(fun):
    @functools.wraps(fun)
    def wrap(*args,**kw):
        start = time.time()
        fun(*args,**kw)
        duration = time.time() - start
        print '<{0:}> -- {1:.2f}'.format(fun.__name__,duration)

class Root_Histogram_Handler(object):
    isDebug = False
    def __init__(self,path_to_root_file,version):
        self.version = version
        self.path_to_root_file = path_to_root_file


    def SetFilter(self,fun):
        self.filter = fun

    def SetBinning(self,xbins):
        self.xbins = xbins
        self.str_xbins = '|'.join(map(str,xbins))


    def Digest(self,path_to_data_base):
        if Root_Histogram_Handler.isDebug:
            os.system('rm %s'%(path_to_data_base))
        self.DB = sql.connect(path_to_data_base)        
        self._write_meta_data()
        self._write_histograms()
        self.DB.commit()

    def GetMetaData(self,version=None):
        cursor = self.DB.cursor()
        if version!=None:
            cursor.execute('''
SELECT * FROM MetaData
WHERE Version=?;
            ''',(version,))
        else:
            cursor.execute('''
SELECT * FROM MetaData
            ''')
        itms = cursor.fetchall()
        if len(itms) == 0:
            return []
        else:
            res = list(itms)
            return res

    def _print_meta(self,meta_data):
        header = self._get_header('MetaData')
        for n,itm in enumerate(meta_data):
            print '{0:<15} {1:}'.format(header[n]+':',itm)

    def _get_header(self,table_name):
        cursor = self.DB.cursor()
        cursor.execute('''
            SELECT * FROM {0:}
            '''.format(table_name))
        header = [ key[0] for key in cursor.description ]
        return header

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
        meta = self.GetMetaData(self.version)
        if len(meta)!=0:
            print 'Version <{0:}> exists already!'
            self._print_meta(meta[0])
            raise ValueError()
        with open(self.path_to_root_file,'rb') as f:
            chunk = f.read()
            hasher = hashlib.md5()
            hasher.update(chunk)
            hash_value = hasher.hexdigest()
        str_time = datetime.datetime.utcnow().strftime("%Y-%m-%d,%a,%H:%M:%S UTC+0")
        cursor.execute('''
INSERT INTO MetaData(Version,Binning,ImportTime,InputFile,HashValue) \
VALUES(?,?,?,?,?);
        ''',(self.version,self.str_xbins,str_time,self.path_to_root_file,hash_value))
        meta = self.GetMetaData(self.version)
        print 'This input going to be processed'
        self._print_meta(meta[0])

    def _write_histograms(self):
        self._initialize_table()
        f = R.TFile(self.path_to_root_file,'read')
        self.entries = []
        self._walker(f)
        cursor = self.DB.cursor()
        cursor.executemany('''
INSERT INTO Data_{version:}({keys:}) \
VALUES({question_marks});
'''.format(version=self.version,keys=self.str_keys,question_marks=self.str_question_marks),self.entries)

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
        self.str_question_marks = '?,?,?,?,?,?'+(',?,?'*nbins)
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
        '''%(self.version,str_value_error.strip(' ')[:-2])        
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

    def _write_entry(self,name,key):
        sample,tp,category,status,description,aux = self.filter(name)        
        if status!=None:
            value_errors = self._read_hist(key)
            values = (sample,tp,category,status,description,aux) + value_errors
            cursor = self.DB.cursor()
            if False:
                cursor.execute('''
    SELECT * FROM Data_{version:}
    WHERE Sample=? AND TP=? AND Category=? AND Status=? AND Description=? AND Aux=?;
                '''.format(version=self.version),(sample,tp,category,status,description,aux))
                entries = cursor.fetchall()
                if len(entries)>=1:
                    for entry in entries:
                        print entry
                    print name
                    raise ValueError("entry already exists!")
            self.entries.append(values)

    def _read_hist(self,key):
        hist = key.ReadObj()
        values = ()
        for nbin in range(len(self.xbins)-1):
            center = (self.xbins[nbin] + self.xbins[nbin+1])/2.
            nbin_h = hist.GetXaxis().FindBin(center)
            value,error = hist.GetBinContent(nbin_h),hist.GetBinError(nbin_h)
            values += (value,error)
        return values