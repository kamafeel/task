package com.asiainfo.dacp.scheduler.service;

import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.context.support.XmlWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

/**
 * jetty服务启动类
 * @author niujz
 * @modify Disable TRACE by zhangqi 
 */
@Service
public class JettyService {
	private static ApplicationContext context;
	@Value("${jetty.port}")
	private int jettyPort;
	private Server server;

	public void startJettyServer(final Class<?>... claz) {
		new Thread() {
			@Override
			public void run() {
				context = new FileSystemXmlApplicationContext(new String[] { "conf/*.xml" });
				server = new Server(jettyPort);
				ServletContextHandler contexts = new ServletContextHandler(ServletContextHandler.SESSIONS | ServletContextHandler.SECURITY);
				contexts.setContextPath("/");
				server.setHandler(contexts);
				AnnotationConfigWebApplicationContext webApplicationContext = new AnnotationConfigWebApplicationContext();
				DispatcherServlet dispatcherServlet = new DispatcherServlet(webApplicationContext);
				XmlWebApplicationContext xmlWebAppContext = new XmlWebApplicationContext();
				xmlWebAppContext.setParent(context);
				xmlWebAppContext.setConfigLocation("");
				xmlWebAppContext.setServletContext(contexts.getServletContext());
				xmlWebAppContext.refresh();
				contexts.setAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE, xmlWebAppContext);
				webApplicationContext.register(claz);
				contexts.addServlet(new ServletHolder(dispatcherServlet), "/*");
				
				
				 /*
			     * <security-constraint>
			     * <web-resource-collection>
			     * <web-resource-name>Disable TRACE</web-resource-name>
			     * <url-pattern>/</url-pattern>
			     * <http-method>TRACE</http-method>
			     * </web-resource-collection>
			     * <auth-constraint/>
			     * </security-constraint>
			     */
			     Constraint constraint = new Constraint();
			     constraint.setName("Disable TRACE");
			     constraint.setAuthenticate(true);
			     
			     ConstraintMapping mapping = new ConstraintMapping();
			     mapping.setConstraint(constraint);
			     mapping.setMethod("TRACE");
			     mapping.setPathSpec("/"); // this did not work same this mapping.setPathSpec("/*");
			     ConstraintSecurityHandler securityHandler = (ConstraintSecurityHandler) contexts.getSecurityHandler();
			     securityHandler.addConstraintMapping(mapping);
				
				
				try {
					server.start();
					server.join();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
	}
}
