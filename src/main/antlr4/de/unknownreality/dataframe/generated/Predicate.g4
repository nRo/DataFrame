grammar Predicate;
 
/*
 * Parser Rules
 */

compilationUnit   : predicate EOF ;

field_filter :
NEGATE? OPEN_BRACKET field_filter CLOSE_BRACKET|
VAR FIELD_OPERATION value |
regex_filter;


regex_filter :
VAR MATCH REGEX;

predicate:
NEGATE? OPEN_BRACKET predicate CLOSE_BRACKET |
OPEN_BRACKET predicate PREDICATE_OPERATION predicate CLOSE_BRACKET|
predicate PREDICATE_OPERATION predicate|
field_filter;


value: NUMBER | BOOLEAN_VALUE | TEXT_VALUE;

/*
 * Lexer Rules
 */
fragment DIGIT : [0-9];
fragment CHAR : [a-zA-Z];
fragment STRING : '\'' ~('\'')* '\'';

fragment VAR_NAME :CHAR+[a-zA-Z0-9]*;
fragment UNESCAPED_STRING :~('\''|' '|')' | '(')+;

fragment EQ : ('EQ' | 'eq' | '=' | '==');
fragment NE : ('NE' | 'ne' | '!=');
fragment LE : ('LE' | 'le' | '<=');
fragment LT : ('LT' | 'lt' | '<');
fragment GE : ('GE' | 'ge' | '>=');
fragment GT : ('GT' | 'gt' | '>');

REGEX : '/' (~('/') | '\\/')+ '/';
MATCH : ('~=' | '~' );


fragment AND : ('AND' | 'and' | '&' | '&&');
fragment OR : ('OR' | 'or' | '|' | '||');
fragment XOR : ('XOR' | 'xor') ;
fragment NOR : ('NOR' | 'nor') ;
OPEN_BRACKET: '(';
CLOSE_BRACKET: ')';

NEGATE: '^';

PREDICATE_OPERATION : AND | OR | XOR | NOR;

FIELD_OPERATION : EQ | NE |LE | LT | GT | GE;

NUMBER : '-'? DIGIT+([.,]DIGIT+)?;
BOOLEAN_VALUE: 'true' | 'false';
TEXT_VALUE : STRING ;
VAR : UNESCAPED_STRING ('.' (UNESCAPED_STRING | STRING))? | STRING;

WHITESPACE : ' ' -> skip ;
