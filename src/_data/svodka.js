const fetch = require('node-fetch');
var Enumerable = require('linq');

const mmq=require('./mmqmodules4.json');



module.exports = async function() {

  const by_weeks=
  {
    "by_weeks": {
      "range": {
        "field": "import_date",
        "keyed":false,
        "ranges":
        [
          {"from":"now-4w/w","to":"now-3w/w"},
          {"from":"now-3w/w","to":"now-2w/w"},
          {"from":"now-2w/w","to":"now-1w/w"},
          {"from":"now-1w/w","to":"now/w"},
          {"from":"now/w","to":"now+1w/w"}
        ]
      }
    }    
  };

  const by_month=
  {
    "by_month": {
      "range": {
        "field": "import_date",
        "keyed":false,
        "ranges":
        [
          {"from":"now-4w/w","to":"now-3w/w"},
          {"from":"now-3w/w","to":"now-2w/w"},
          {"from":"now-2w/w","to":"now-1w/w"},
          {"from":"now-1w/w","to":"now/w"},
          {"from":"now/w","to":"now+1w/w"}
        ]
      }
    }    
  };

  const MAXD=
  {
    "MAXD": {  "max": { "field": "import_date"   }   }
  };

    const body = { 
        size: 0,
        aggs:{
            modules:{
                terms:{
                    field: "type.keyword",
                    order: { _key: "asc"   },
                    size: 1000
                  },
    
                  "aggs": {  ...MAXD ,  ...by_month , ...by_weeks  }              
    
            }
        }
    
    };
    

    const response = await fetch('http://elasticsearch0.ngrok.io/tndr-stored-*/_search',{ method:"POST", body: JSON.stringify(body), headers: {'Content-Type': 'application/json'}  });
    const data = await response.json();

    //console.log(data);

//    Enumerable.from(data.aggregations.modules.buckets).select(_t=>`${_t.key} - ${_t.doc_count}  ${JSON.stringify(_t.by_month.buckets.length)}`).log().toJoinedString();



var data1=Enumerable.from(data.aggregations.modules.buckets).select(_t=>`${_t.key} - ${_t.doc_count}  ${JSON.stringify(_t.by_month.buckets.length)}`).toArray();
var data2=Enumerable.from(data.aggregations.modules.buckets).select(_t=> ({ Module: _t.key, Cnt:_t.doc_count , Length:_t.by_month.buckets.length})).toArray();

var data20=Enumerable.from(data.aggregations.modules.buckets).select(_t=> ({ Module: _t.key, Cnt:_t.doc_count , Length:_t.by_month.buckets.length, MDate: new Date(_t.MAXD.value) }));

var ttx=_src=>data20.firstOrDefault(t=>t.Module.toLowerCase()==_src.toLowerCase())||{ Module:_src,cnt:0,Lengt:0 ,MDate: null};

var ttx2=_t=>{ var x=ttx(_t.Modules); console.log(x); return {Module: _t.Modules, State: (_t.Modules.match(/^[a-zA-Z]{2}/g)||["??"])[0].toUpperCase() ,    Programmer: _t.Programmer.trim() , Cnt: x.Cnt, Length: x.Length,MDate: x.MDate};}


var data3=Enumerable.from(mmq).toArray();

var data4=Enumerable.from(mmq).select(t=>({Module: t.Modules, Programmer: t.Programmer})).toArray();

var data41=Enumerable.from(mmq).select(t=>(ttx2(t))).toArray();


var mmqbase=Enumerable.from(mmq);
var dtbase=Enumerable.from(data.aggregations.modules.buckets);


var dmmq=mmqbase.select(t=>({Module: t.Modules, Programmer: t.Programmer.trim() }));

//var qmmq=_namemod=>dmmq.firstOrDefault(t=>t.Module.toLowerCase()==_namemod.toLowerCase())||{Module:_namemod,Programmer:""};


var qmmq=_namemod=>{

  var _rez=dmmq.firstOrDefault(t=>t.Module.toLowerCase()==_namemod.toLowerCase());

  if (!_rez)
  {
    var _mod0=(_namemod.match(/^(.+?)Module\s*$/)||["?","?"])[1].toLowerCase();
    _rez=dmmq.firstOrDefault(t=>t.Module.toLowerCase()==_mod0);
  }

  _rez=_rez||{Module:_namemod,Programmer:""};

  return  _rez;

};


var _getState=_src=>(_src.match(/^[a-zA-Z]{2}/g)||["??"])[0].toUpperCase();

var _normDate=_src=>{
  if (!_src)  return  "";
  //console.log(`>>>>${_src}<<<<`)
  var _m=_src.match(/(\d{4})-(\d{2})-(\d{2})T/);
  if (_m==null)
  {
    return  "";
  } 

  var _rez=`${_m[3]}/${_m[2]}/${_m[1]}`;

  return  _rez;

};

//var qmmq1= _tt=> ({ Module: _tt.key, State: (_tt.key.match(/^[a-zA-Z]{2}/g)||["??"])[0].toUpperCase()    , Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: new Date(_tt.MAXD.value), Programmer: qmmq(_tt.key).Programmer  });
//var qmmq1= _tt=> ({ Module: _tt.key, State: _getState(_tt.key)  , Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: new Date(_tt.MAXD.value), Programmer:    (qmmq(_tt.key).Programmer.match(/^([A-Z])/g)||[""])[0]       });
//var qmmq1= _tt=> ({ Module: _tt.key, State: _getState(_tt.key)  , Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: new Date(_tt.MAXD.value).toLocaleDateString('en'), Programmer:    (qmmq(_tt.key).Programmer.match(/^([A-Z])/g)||[""])[0]       });
var _bucks=_tt=>({  
                    buck00:_tt.by_month.buckets[0], 
                    buck01:_tt.by_month.buckets[1], 
                    buck02:_tt.by_month.buckets[2], 
                    buck03:_tt.by_month.buckets[3], 
                    buck04:_tt.by_month.buckets[4] 
                });

  var _bucks2=_tt=> ({  buckets: Enumerable.from(_tt.by_month.buckets).orderBy(x=>x.to).skip(1).toArray()  });

var qmmq10= _tt=> ({ Module: _tt.key, State: _getState(_tt.key)  , Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: _tt.MAXD.value, Programmer:    (qmmq(_tt.key).Programmer.match(/^([A-Z])/g)||[""])[0]       });
var qmmq1= _tt=> ({  ...qmmq10(_tt),..._bucks(_tt),..._bucks2(_tt)  });



//var qmmq1= _tt=> ({ Module: _tt.key, State: _getState(_tt.key)  , Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: Date.parse(new Date(_tt.MAXD.value).toLocaleDateString('en')), Programmer:    (qmmq(_tt.key).Programmer.match(/^([A-Z])/g)||[""])[0]       });

//var dt20=Enumerable.from(data.aggregations.modules.buckets).select(_t=> ({ Module: _t.key, Cnt:_t.doc_count , Length:_t.by_month.buckets.length, MDate: new Date(_t.MAXD.value), Programmer: qmmq(_t.key).Programmer  })).toArray();

//var dt20=Enumerable.from(data.aggregations.modules.buckets).select(_t=> qmmq1(_t) ).toArray();

var dt20b=dtbase.select(_t=> qmmq1(_t) );
var dt20=dt20b.toArray();


var _restMname=_src=>{

  var _name=_src.trim();

  if (_name.match(/^[^\n]+?Module$/i))
  {
    return  _name;
  }

  if (!_name.match(/^[^\n]+$/i))
  {
    return  "";
  }


  return  `${_name}Module`;
};

var qmmq2= _tt=> ({ Module: _restMname(_tt.Modules), State: _getState(_tt.Modules)  , /* Cnt:_tt.doc_count , Length:_tt.by_month.buckets.length, MDate: new Date(_tt.MAXD.value), */ Programmer:   (_tt.Programmer.match(/^([A-Z])/g)||[""])[0]     });

var tytb=
mmqbase
        .where(t=>dtbase.firstOrDefault(w=>w.key.toLowerCase()==_restMname(t.Modules).toLowerCase())==null)
        .where(x=>(_restMname(x.Modules)))
//        .select(x=>({Module: _restMname(x.Modules) }))
//        .where(x=>(x.Module))
        .select(qmmq2);

var tyt=tytb.toArray();

 //console.log("?|?|?  ",tyt.length);    
 
 
 return tytb.union(dt20b).orderBy(t=>t.Module).toArray();

 return tyt;


//console.log(data41);

// return data41;

return dt20;


}