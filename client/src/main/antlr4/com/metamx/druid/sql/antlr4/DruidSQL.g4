
grammar DruidSQL;

/*
@lexer::header {
  package com.metamx.druid.sql.antlr;
}
*/
/*
TODO: handle both multiply / division, addition - subtraction
unary minus
*/

@header {
import com.metamx.druid.aggregation.post.*;
import com.metamx.druid.aggregation.*;
import com.metamx.druid.query.filter.*;
import com.google.common.base.*;
import com.google.common.collect.Lists;
import java.util.*;
import org.joda.time.*;
}

@parser::members {
    public Map<String, AggregatorFactory> aggregators = new LinkedHashMap<String, AggregatorFactory>();
    public List<PostAggregator> postAggregators = new LinkedList<PostAggregator>();
    public DimFilter filter;
    public List<org.joda.time.Interval> intervals;
    
    String dataSourceName = null;
    
    public String getDataSource() {
        return dataSourceName;
    }
    
    public String unescape(String quoted) {
        String unquote = quoted.replaceFirst("^'(.*)'\$", "\$1");
        return unquote.replace("''", "'");
    }

    AggregatorFactory evalAgg(String name, int fn) {
        switch (fn) {
            case SUM: return new DoubleSumAggregatorFactory("doubleSum:"+name, name);
            case MIN: return new MinAggregatorFactory("min:"+name, name);
            case MAX: return new MaxAggregatorFactory("max:"+name, name);
            case COUNT: return new CountAggregatorFactory(name);
        }
        throw new IllegalArgumentException("Unknown function [" + fn + "]"); 
    }
    
    PostAggregator evalArithmeticPostAggregator(PostAggregator a, List<Token> ops, List<PostAggregator> b) {
        if(b.isEmpty()) return a;
        else {            
            int i = 0;
            
            PostAggregator root = a;
            while(i < ops.size()) {
                List<PostAggregator> list = new LinkedList<PostAggregator>();
                List<String> names = new LinkedList<String>();

                names.add(root.getName());
                list.add(root);
                
                Token op = ops.get(i);
                
                while(i < ops.size() && ops.get(i).getType() == op.getType()) {
                    PostAggregator e = b.get(i);
                    list.add(e);
                    names.add(e.getName());
                    i++;
                }
                
                root = new ArithmeticPostAggregator("("+Joiner.on(op.getText()).join(names)+")", op.getText(), list);
            }
            
            return root;
        }
    }
}


AND: 'and';
OR: 'or';
SUM: 'sum';
MIN: 'min';
MAX: 'max';
COUNT: 'count';
AS: 'as';
OPEN: '(';
CLOSE: ')';
STAR: '*';
PLUS: '+';
MINUS: '-';
DIV: '/';

IDENT : (LETTER|DIGIT)(LETTER | DIGIT | '_')* ;
QUOTED_STRING : '\'' ( ESCqs | ~'\'' )* '\'' ;
ESCqs : '\'' '\'';

fragment DIGIT : '0'..'9';
fragment LETTER : 'a'..'z' | 'A'..'Z';

INTEGER : [+-]?DIGIT+ ;
DOUBLE: [+-]?DIGIT*\.?DIGIT+(EXPONENT)?;
EXPONENT: ('e') ('+'|'-')? ('0'..'9')+;

WS :  (' ' | '\t' | '\r' '\n' | '\n' | '\r')+ -> skip;



query
    : select_stmt where_stmt
    ;

select_stmt
    : 'select' e+=aliasedExpression (',' e+=aliasedExpression)* 'from' datasource {
        for(AliasedExpressionContext a : $e) { postAggregators.add(a.p); }
        this.dataSourceName = $datasource.text;
    }
    ;

where_stmt
    : 'where' timeAndDimFilter
    ;

datasource
    : IDENT 
    ;

aliasedExpression returns [PostAggregator p]
    : expression ( AS^ name=IDENT )? { $p = $expression.p; }
    ;

expression returns [PostAggregator p]
    : additiveExpression { $p = $additiveExpression.p; }
    ;

additiveExpression returns [PostAggregator p]
    : a=multiplyExpression (( ops+=PLUS^ | ops+=MINUS^ ) b+=multiplyExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(MultiplyExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

multiplyExpression returns [PostAggregator p]
    : a=unaryExpression ((ops+= STAR | ops+=DIV ) b+=unaryExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(UnaryExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

unaryExpression returns [PostAggregator p]
    : MINUS e=unaryExpression {
        $p = new ArithmeticPostAggregator(
            "-"+$e.p.getName(),
            "*",
            Lists.newArrayList($e.p, new ConstantPostAggregator("-1.0", -1.0))
        );
    }
    | PLUS e=unaryExpression { $p = $e.p; }
    | primaryExpression { $p = $primaryExpression.p; }
    ;

primaryExpression returns [PostAggregator p]
    : constant { $p = $constant.c; }
    | aggregate {
        aggregators.put($aggregate.agg.getName(), $aggregate.agg);
        $p = new FieldAccessPostAggregator($aggregate.agg.getName(), $aggregate.agg.getName());
    }
    | OPEN! e=expression CLOSE! { $p = $e.p; }
    ;

aggregate returns [AggregatorFactory agg]
    : fn=( SUM^ | MIN^ | MAX^ ) OPEN! name=(IDENT|COUNT) CLOSE! { $agg = evalAgg($name.text, $fn.type); }
      | fn=COUNT OPEN! STAR CLOSE! { $agg = evalAgg("count", $fn.type); }
    ;

/*
fieldAccess returns [PostAggregator p]
    : IDENT { $p = new FieldAccessPostAggregator($IDENT.text, $IDENT.text); }
    ;
*/

constant returns [ConstantPostAggregator c]
    : value=INTEGER { int v = Integer.parseInt($value.text); $c = new ConstantPostAggregator(Integer.toString(v), v); }
    | value=DOUBLE  { double v = Double.parseDouble($value.text); $c = new ConstantPostAggregator(Double.toString(v), v); }
    ;

timeAndDimFilter
    : t=timeFilter (AND f=dimFilter)? {
        if($f.ctx != null) this.filter = $f.filter;
        this.intervals = Lists.newArrayList($t.interval);
    }
    | (f=dimFilter)? AND t=timeFilter {
        if($f.ctx != null) this.filter = $f.filter;
        this.intervals = Lists.newArrayList($t.interval);
    }
    ;

dimFilter returns [DimFilter filter]
    : e=orDimFilter { $filter = $e.filter; }
    ;

orDimFilter returns [DimFilter filter]
    : a=andDimFilter (OR^ b+=andDimFilter)* {
        if($b.isEmpty()) $filter = $a.filter;
        else {
            List<DimFilter> rest = new ArrayList<DimFilter>();
            for(AndDimFilterContext e : $b) rest.add(e.filter);
            $filter = new OrDimFilter(Lists.asList($a.filter, rest.toArray(new DimFilter[]{})));
        }
    }
    ;

andDimFilter returns [DimFilter filter]
    : a=primaryDimFilter (AND^ b+=primaryDimFilter)* {
        if($b.isEmpty()) $filter = $a.filter;
        else {
            List<DimFilter> rest = new ArrayList<DimFilter>();
            for(PrimaryDimFilterContext e : $b) rest.add(e.filter);
            $filter = new AndDimFilter(Lists.asList($a.filter, rest.toArray(new DimFilter[]{})));
        }
    }
    ;

primaryDimFilter returns [DimFilter filter]
    : e=selectorDimFilter { $filter = $e.filter; }
    | OPEN! f=dimFilter CLOSE! { $filter = $f.filter; }
    ;

selectorDimFilter returns [SelectorDimFilter filter]
    : dimension=IDENT '=' value=QUOTED_STRING {
        $filter = new SelectorDimFilter($dimension.text, unescape($value.text));
    }
    ;

timeFilter returns [org.joda.time.Interval interval]
    : 'timestamp' 'between' s=timestamp AND e=timestamp {
        $interval = new org.joda.time.Interval(new DateTime(unescape($s.text)), new DateTime(unescape($e.text)));
    }
    ;

timestamp
    : INTEGER | QUOTED_STRING
    ;
