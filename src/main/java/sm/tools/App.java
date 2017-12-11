package sm.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import sm.tools.veda_client.Individual;
import sm.tools.veda_client.Resource;
import sm.tools.veda_client.Type;
import sm.tools.veda_client.VedaConnection;

/**
 * Hello world!
 *
 */
public class App 
{
	public static String getActualDocument(String baUri, Connection docDbConn) throws SQLException {
		String queryStr = "SELECT content, recordid, timestamp FROM objects WHERE objectId = ? AND actual = 1 AND isDraft = 0";

		PreparedStatement ps = docDbConn.prepareStatement(queryStr);
		ps.setString(1, baUri);
		// ps.setLong(2, timestamp);
		ResultSet rs = ps.executeQuery();
		String res = "";
		if (rs.next()) {
			res = rs.getString(1);
			String recordid = rs.getString(2);
			recordid.length();
		/*	doc = (XmlDocument) XmlUtil.unmarshall(xml);
			res = new Pair<XmlDocument, Long>(doc, rs.getLong(3));*/
			
		} 
		
		rs.close();
		ps.close();
		return res;
	}
    public static void main( String[] args )
    {
    	HashMap<String, String> config = new HashMap<String, String>();
    	try {
    		BufferedReader br = new BufferedReader(new FileReader("config.conf"));
    		
    		int count = 1;
    	    for(String line; (line = br.readLine()) != null; count++) {
    	    	int idx = line.indexOf("#");
    	    	if (idx >= 0) {
    	    		line = line.substring(0, idx);
    	    	}
    	    
    	    	line = line.trim();
    	    	if (line.length() == 0)
    	    		continue;
    	    	
    	        idx = line.indexOf("=");
    	        if (idx < 0) {
    	        	System.err.println(String.format("ERR! Invalid line %d, "
    	        			+ "'=' was not found: %s", count, line));
    	        	return;
    	        }
    	        

    	        String paramName = line.substring(0, idx);
    	        String paramVal = line.substring(idx + 1);
    	        
    	        config.put(paramName, paramVal);
    	    }
    	} catch (Throwable t) {
    		System.err.println("ERR! Cannot read config file: " + t.getMessage());
    		return;
    	}
    	
    	String veda_url;

   		if (!config.containsKey("veda")) {
   			System.err.println("ERR! Config key 'veda' is not set");
   			return;
   		}
   		veda_url = config.get("veda");
    	
   		VedaConnection veda = null;
    	try {
    		veda = new VedaConnection(veda_url, "ImportDMSToVeda",
    			"a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3");
    	} catch (Exception e) {
    		System.err.println("ERR! Can not connect to Veda");
    		e.printStackTrace();
    		return;
    	}
    	
    	Connection docDbConn;
    	String docDbUser, docDbPassword, docDbUrl;
    	try {
    		docDbUser = config.get("doc_db_user");
    		if (!config.containsKey("doc_db_user")) {
    			System.err.println("ERR! Config key 'doc_db_user' is not set");
    			return;
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    		return;
    	}
    	
    	try {
    		docDbPassword = config.get("doc_db_password");
    		if (!config.containsKey("doc_db_password")) {
    			System.err.println("ERR! Config key 'doc_db_password' is not set");
    			return;
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    		return;
    	}
    	
    	try {
    		docDbUrl = config.get("doc_db_url");
    	} catch (Exception e) {
    		System.err.println("ERR! Config key 'doc_db_url' is not set");
    		return;
    	}
    	

    	try {
    		docDbConn = DriverManager.getConnection("jdbc:mysql://" + docDbUrl, docDbUser, docDbPassword);
    	} catch (Exception e) {
    		System.err.println("ERR! Can not connect to documents database");
    		e.printStackTrace();
    		return;
    	}
    	
    	XPathFactory factory =  XPathFactory.newInstance();
		XPath xpath = factory.newXPath();
		XPathExpression expr = null;
		Object result = null;
		NodeList list = null;
    	try {
    		String[] uris = veda.query("'rdf:type'==='mnd-s:AdditionalAgreement' ) && ( 'v-s:backwardTarget'== 'd:zzzz')");
    		for (int i = 0; i < uris.length; i++) {
    			String baUri = uris[i].substring(2);	
    			System.out.println(baUri);
				String xml = getActualDocument(baUri, docDbConn);
				   			
				DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    			Document doc = docBuilder.parse(new ByteArrayInputStream(xml.getBytes()));
    			
    			expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='add_to_contract']");
    			result = expr.evaluate(doc, XPathConstants.NODESET);
    			list = (NodeList)result;
    			if (list.getLength() > 0) {
    				Node node = list.item(0);
    				if (node != null) {
    					NodeList children = node.getParentNode().getChildNodes();
    					for (int j = 0; j < children.getLength(); j++) {
    						Node child = children.item(j);
    						if (child.getNodeName() == "linkValue") {
    							String link = child.getTextContent();
    							String contractXml = getActualDocument(link, docDbConn);
    							doc = docBuilder.parse(new ByteArrayInputStream(contractXml.getBytes()));
    							expr = xpath.compile("//xmlDocument/xmlAttributes/xmlAttribute/code[text()='inherit_rights_from']");
    			    			result = expr.evaluate(doc, XPathConstants.NODESET);
    			    			list = (NodeList)result;
    			    			String inherit_rights_from = null;
    			    			if (list.getLength() > 0) {
    			    				node = list.item(0);
    			    				if (node != null) {
	    			    				children = node.getParentNode().getChildNodes();
	    		    					for (int k = 0; k < children.getLength(); k++) {
	    		    						child = children.item(k);
	    		    						if (child.getNodeName() == "linkValue") {
	    		    							inherit_rights_from = child.getTextContent();
	    		    							break;
	    		    						}
	    		    					}
    			    				}
    			    			}
    			    			
    			    			if (inherit_rights_from == null)
    			    				inherit_rights_from = link;
    			    			
    			    			inherit_rights_from = "d:" + inherit_rights_from;
    			    			Individual indv = veda.getIndividual(uris[i]);
    			    			indv.setProperty("v-s:backwardTarget", new Resource(inherit_rights_from,Type._Uri));
    			    			veda.putIndividual(indv, true, 0);
    			    			break;
    						}
    					}
    				}
    			}
	    	}
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
