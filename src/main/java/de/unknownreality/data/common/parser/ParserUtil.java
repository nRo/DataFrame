package de.unknownreality.data.common.parser;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Alex on 04.06.2015.
 */
public class ParserUtil {
    private static Map<Class<?>,Parser> parserMap;
    private static  Map<Class<?>,Parser> getParserMap(){
        if(parserMap == null){
            parserMap = new ConcurrentHashMap<>();
            init();
        }
        return parserMap;
    }


    //init default parsers
    private static void init(){
        parserMap.put(String.class,new Parser<String>(){
            @Override
            public String parse(String s) {
                return s;
            }
        });

        parserMap.put(Double.class,new Parser<Double>(){
            @Override
            public Double parse(String s) {
                return Double.parseDouble(s);
            }
        });


        parserMap.put(Integer.class,new Parser<Integer>(){
            @Override
            public Integer parse(String s) {
                return Integer.parseInt(s);
            }
        });


        parserMap.put(Float.class,new Parser<Float>(){
            @Override
            public Float parse(String s) {
                return Float.parseFloat(s);
            }
        });

        parserMap.put(Long.class,new Parser<Long>(){
            @Override
            public Long parse(String s) {
                return Long.parseLong(s);
            }
        });

        parserMap.put(Boolean.class,new Parser<Boolean>(){
            @Override
            public Boolean parse(String s) {
                return Boolean.parseBoolean(s);
            }
        });

        parserMap.put(Short.class,new Parser<Short>(){
            @Override
            public Short parse(String s) {
                return Short.parseShort(s);
            }
        });

        parserMap.put(Character.class,new Parser<Character>(){
            @Override
            public Character parse(String s) {
                return s.charAt(0);
            }
        });

        parserMap.put(Byte.class,new Parser<Byte>(){
            @Override
            public Byte parse(String s) {
                return Byte.parseByte(s);
            }
        });

    }

    private static <T> T parseArray(Class<T> cl,String x)  throws ParseException{
            Parser p = getParserMap().get(cl.getComponentType());
            Class cc = cl.getComponentType();
            String[] vals = x.split("[;,|]");
            Object r = Array.newInstance(cc, vals.length);
            for(int i = 0; i < vals.length;i++){
                Array.set(r,i,p.parse(vals[i]));
            }
            return (T)r;
    }


    public static <T> T parse(Class<T> cl,String x) throws ParseException, ParserNotFoundException {
        if((cl.isArray() && !getParserMap().containsKey(cl.getComponentType())) &&  !getParserMap().containsKey(cl)){
            throw new ParserNotFoundException(cl);
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            Parser<T> p = getParserMap().get(cl);
            return p.parse(x);
        }
        catch (Exception e){
            throw new ParseException("error parsing '"+x+"' to "+cl.getName(),0);
        }
    }

    public static boolean hasParser(Class<?> cl){
        return getParserMap().get(cl) != null;
    }

    public static <T> Parser<T> getParser(Class<T> cl) throws ParserNotFoundException{
        return getParserMap().get(cl);
    }

    public static <T> Parser<T> findParserOrNull(Class<T> cl){
        return getParserMap().get(cl);
    }

    public static <T> T parseOrNull(Class<T> cl,String x){
        if((cl.isArray() && !getParserMap().containsKey(cl.getComponentType())) && !getParserMap().containsKey(cl)){
            return null;
        }
        try {
            if (cl.isArray()) {
                return parseArray(cl, x);
            }
            Parser<T> p = getParserMap().get(cl);
            return p.parse(x);
        }
        catch (Exception e){
            return null;
        }
    }


    public static  <T> void addParser(Class<T> c, Parser p){
        getParserMap().put(c, p);
    }
}
