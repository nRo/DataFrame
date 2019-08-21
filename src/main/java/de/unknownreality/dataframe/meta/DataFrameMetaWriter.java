/*
 *
 *  * Copyright (c) 2019 Alexander Grün
 *  *
 *  * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  * of this software and associated documentation files (the "Software"), to deal
 *  * in the Software without restriction, including without limitation the rights
 *  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  * copies of the Software, and to permit persons to whom the Software is
 *  * furnished to do so, subject to the following conditions:
 *  *
 *  * The above copyright notice and this permission notice shall be included in all
 *  * copies or substantial portions of the Software.
 *  *
 *  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  * SOFTWARE.
 *
 */

package de.unknownreality.dataframe.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;

/**
 * Created by Alex on 07.06.2016.
 */
public class DataFrameMetaWriter {
    private static final Logger logger = LoggerFactory.getLogger(DataFrameMetaWriter.class);


    private DataFrameMetaWriter(){}
    /**
     * Writes a data frame meta information to a file
     *
     * @param metaFile data frame meta information
     * @param file     target file
     */
    public static void write(DataFrameMeta metaFile, File file) {
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element rootElement = doc.createElement("dataFrameMeta");
            doc.appendChild(rootElement);

            Element dataFrameElement = doc.createElement("dataFrame");
            dataFrameElement.setAttribute("size",Integer.toString(metaFile.getSize()));
            rootElement.appendChild(dataFrameElement);

            Element readerBuilder = doc.createElement("readerBuilder");
            rootElement.appendChild(readerBuilder);

            Attr attr = doc.createAttribute("class");
            attr.setValue(metaFile.getReadFormatClass().getCanonicalName());
            readerBuilder.setAttributeNode(attr);

            for (String attrName : metaFile.getAttributes().keySet()) {
                Element readerAttribute = doc.createElement("readerAttribute");
                Attr name = doc.createAttribute("name");
                name.setValue(attrName);
                readerAttribute.setAttributeNode(name);
                Attr value = doc.createAttribute("value");
                value.setValue(metaFile.getAttributes().get(attrName));
                readerAttribute.setAttributeNode(value);
                readerBuilder.appendChild(readerAttribute);
            }

            Element columns = doc.createElement("columns");
            rootElement.appendChild(columns);

            for (String colName : metaFile.getColumns().keySet()) {
                Element colAttribute = doc.createElement("column");
                Attr name = doc.createAttribute("name");
                name.setValue(colName);
                colAttribute.setAttributeNode(name);

                Attr type = doc.createAttribute("type");
                type.setValue(metaFile.getColumns().get(colName).getCanonicalName());
                colAttribute.setAttributeNode(type);
                columns.appendChild(colAttribute);
            }

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            DOMSource source = new DOMSource(doc);
            if (!file.getParentFile().isDirectory()) {
                file.getParentFile().mkdirs();
            }
            StreamResult result = new StreamResult(file);
            transformer.transform(source, result);

        } catch (Exception e) {
            logger.error("error writing data frame meta file", e);
        }
    }
}
