package de.unknownreality.dataframe.meta;

import de.unknownreality.dataframe.common.ReaderBuilder;
import de.unknownreality.dataframe.DataFrameColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.*;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMetaReader {
    private static Logger logger = LoggerFactory.getLogger(DataFrameMetaReader.class);

    /**
     * Map containing legacy package names.
     * This map is used to rename Reader and Column classes in an old Meta file.
     */
    private static final Map<String,String> LEGACY_PACKAGES = new TreeMap<>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return -Integer.compare(o1.length(),o2.length());
        }
    });
    static{
        LEGACY_PACKAGES.put("de.unknownreality.data.","de.unknownreality.dataframe.");
        LEGACY_PACKAGES.put("de.unknownreality.data.frame.","de.unknownreality.dataframe.");
    }

    private static String remapLegacyPackages(String className){
        for(String legacyPackage : LEGACY_PACKAGES.keySet()){
            if(className.contains(legacyPackage)){
                className = className.replace(legacyPackage,LEGACY_PACKAGES.get(legacyPackage));
                logger.warn("old package name found '{}'",legacyPackage);
            }
        }
        return className;
    }

    private static Class<? extends ReaderBuilder> findReaderBuilderClass(Element readerBuilder) throws DataFrameMetaReaderException {
        String rbClassString = readerBuilder.getAttribute("class");
        if (rbClassString == null) {
            throw new DataFrameMetaReaderException("no readerBuilder class attribute found");
        }
        return parseChildClass(rbClassString, ReaderBuilder.class);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? extends T> parseChildClass(String clName, Class<T> parentType) throws DataFrameMetaReaderException {
        clName = remapLegacyPackages(clName);
        Class<?> cl;
        try {
            cl = Class.forName(clName);
        } catch (ClassNotFoundException e) {
            throw new DataFrameMetaReaderException(String.format("class not found: %s", clName, e));
        }
        if (!parentType.isAssignableFrom(cl)) {
            throw new DataFrameMetaReaderException(String.format("%s does not extend %s",
                    clName, parentType.getCanonicalName()));
        }
        return (Class<? extends T>) cl;
    }

    private static Map<String, String> findReaderBuilderAttributes(Element readerBuilder) throws DataFrameMetaReaderException {
        Map<String, String> readerBuilderAttributes = new HashMap<>();
        NodeList attributes = readerBuilder.getElementsByTagName("readerAttribute");
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attributeNode = attributes.item(i);
            if (attributeNode.getNodeType() == Node.ELEMENT_NODE) {
                Element attribute = (Element) attributeNode;
                String name = attribute.getAttribute("name");
                if (name == null) {
                    throw new DataFrameMetaReaderException("no readerAttribute name attribute found");
                }
                String value = attribute.getAttribute("value");
                if (value == null) {
                    throw new DataFrameMetaReaderException("no readerAttribute value attribute found");
                }
                readerBuilderAttributes.put(name, value);
            } else {
                throw new DataFrameMetaReaderException("error parsing attributeElement element");
            }
        }
        return readerBuilderAttributes;
    }

    private static LinkedHashMap<String, Class<? extends DataFrameColumn>> findColumns(Element columns) throws DataFrameMetaReaderException {
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columnsMap = new LinkedHashMap<>();
        NodeList columnNodes = columns.getElementsByTagName("column");
        for (int i = 0; i < columnNodes.getLength(); i++) {
            Node columnNode = columnNodes.item(i);
            if (columnNode.getNodeType() == Node.ELEMENT_NODE) {
                Element column = (Element) columnNode;
                String name = column.getAttribute("name");
                if (name == null) {
                    throw new DataFrameMetaReaderException("no column name attribute found");
                }
                String type = column.getAttribute("type");
                if (type == null) {
                    throw new DataFrameMetaReaderException("no column type attribute found");
                }
                Class<? extends DataFrameColumn> cl = parseChildClass(type, DataFrameColumn.class);
                columnsMap.put(name, cl);
            } else {
                throw new DataFrameMetaReaderException("error parsing column element");
            }
        }
        return columnsMap;
    }


    public static DataFrameMeta read(File file) throws DataFrameMetaReaderException {
        try {
            return read(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new DataFrameMetaReaderException("error reading the meta file",e);
        }
    }
    public static DataFrameMeta read(InputStream is) throws DataFrameMetaReaderException {
        Document doc;
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            doc = docBuilder.parse(is);
            doc.getDocumentElement().normalize();
        } catch (SAXException | ParserConfigurationException e) {
            throw new DataFrameMetaReaderException("error parsing dataFrameMeta file ", e);
        } catch (IOException e) {
            throw new DataFrameMetaReaderException("error reading dataFrameMeta file ", e);
        }
        Class<? extends ReaderBuilder> readerBuilderClass;
        Map<String, String> readerBuilderAttributes;
        LinkedHashMap<String, Class<? extends DataFrameColumn>> columns;

        NodeList readBuilderElements = doc.getElementsByTagName("readerBuilder");
        if (readBuilderElements.getLength() == 0) {
            throw new DataFrameMetaReaderException("no readerBuilder element found");
        }
        if (readBuilderElements.getLength() > 1) {
            throw new DataFrameMetaReaderException("multiple readerBuilder elements found");
        }

        Node readerBuilderNode = readBuilderElements.item(0);
        if (readerBuilderNode.getNodeType() == Node.ELEMENT_NODE) {
            Element readerBuilder = (Element) readerBuilderNode;
            readerBuilderClass = findReaderBuilderClass(readerBuilder);
            readerBuilderAttributes = findReaderBuilderAttributes(readerBuilder);
        } else {
            throw new DataFrameMetaReaderException("error parsing readerBuilder element");
        }

        NodeList columnsElements = doc.getElementsByTagName("columns");
        if (columnsElements.getLength() == 0) {
            throw new DataFrameMetaReaderException("no columns element found");
        }
        if (columnsElements.getLength() > 1) {
            throw new DataFrameMetaReaderException("multiple columns elements found");
        }
        Node columnsNode = columnsElements.item(0);
        if (columnsNode.getNodeType() == Node.ELEMENT_NODE) {
            Element columnsElement = (Element) columnsNode;
            columns = findColumns(columnsElement);
        } else {
            throw new DataFrameMetaReaderException("error parsing columns element");
        }
        return new DataFrameMeta(columns, readerBuilderClass, readerBuilderAttributes);

    }

    public static class DataFrameMetaReaderException extends Exception {
        public DataFrameMetaReaderException() {
        }

        public DataFrameMetaReaderException(String message) {
            super(message);
        }

        public DataFrameMetaReaderException(String message, Exception parent) {
            super(message, parent);
        }
    }
}
