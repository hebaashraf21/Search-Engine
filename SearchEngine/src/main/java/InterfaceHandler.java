import java.io.IOException;
import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import javax.servlet.*;
import javax.servlet.http.*;
public class InterfaceHandler extends HttpServlet{
    //  QueryProcessor Qr;

    public void doGet(HttpServletRequest request,HttpServletResponse response) throws IOException {
        String input=request.getParameter("Searchvalue");
        System.out.println(input);

        //Qr=new QueryProcessor(query);
        //ArrayList<FinalLinks>results=Qr.queryProcessorLogic();


        //query processor
        ArrayList<FinalLinks> results=new ArrayList<>();
        FinalLinks r1=new FinalLinks("www.43rf.com","kkkkkkkkkk","jaskhfjkdsfhskdjfhsdkjfhskdfhskdfjhsf");
        FinalLinks r2=new FinalLinks("www.l3r4.com","wwwwwwwwww","jaskhfjkdsfhskdjfhsdkjfhskdfhskdfjhsf");

        FinalLinks r3=new FinalLinks("www.34x.com.lof","rrrrrrrrrrr","jaskhfjkdsfhskdjfhsdkjfhskdfhskdfjhsf");
        results.add(r1);
        results.add(r2);
        results.add(r3);

        response.setContentType ("text/html");

        String page =  "<!DOCTYPE html>\n" +
                "<html>\n" +
                "    <head>\n" +
                "    <meta charset=\"utf-8\">\n" +
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n" +
                "  <link rel=\"stylesheet\" href=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css\">\n" +
                "  <link rel=\"stylesheet\" href=\"https://use.fontawesome.com/releases/v5.6.3/css/all.css\" integrity=\"sha384-UHRtZLI+pbxtHCWp1t77Bi1L4ZtiqrqD80Kn4Z8NTSRyMA2Fd33n5dQ8lWUE00s/\" crossorigin=\"anonymous\"></head>\n" +
                "  <script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js\"></script>\n" +
                "  <script src=\"https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.16.0/umd/popper.min.js\"></script>\n" +
                "  <script src=\"https://maxcdn.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js\"></script>\n" +
                "    <style>\n" +

                "        body {\n" +
                "            padding: 0%;\n" +
                "        }\n" +

                "       .page-numbers{\n" +
                //"            color: blue;\n" +
                //"            padding-left: 90%;\n" +
                //"            length: 10px;\n" +
                //"            width: 10px;\n" +
                "             border: none;\n"+
                "             color: blue;\n"+
                "               padding: 10px 24px;\n"+
                "               margin: 4px 2px;\n"+
                "               text-align: center;\n"+
                "              text-decoration: none;\n"+
                //"             display: inline;\n"+
                "               font-size: 16px;\n"+
                "               cursor: pointer;\n"+
                "            background-color: white;\n" +

                "        }\n" +

                "       .results{\n" +
                "            padding: 5%;\n" +
                "            padding-top: 0%;\n" +
                "            padding-left: 10%;\n" +
                "            width:80%;\n" +
                "            display:flex%;\n" +
                "        }\n" +
                "       .div-pages{\n" +
                "            padding: 1%;\n" +
                "            padding-top: 0%;\n" +
                "            padding-left: 95%;\n" +
                "            display:inline%;\n" +
                "        }\n" +


                "        }\n" +
                "        #butt li{\n     "+
                "             display: inline;\n"+
                "        }\n" +

                "        #div-pages{\n" +
                "             display: inline;\n"+
                "        }\n" +
                "        .upper-part{\n" +
                "            padding-top: 40px;\n" +
                "            padding-left: 10%;\n" +
                "        }\n" +
                "        .list-group{\n" +
                "            padding-bottom: 20px;\n" +
                //"            padding-left: 10%;\n" +
                "        }\n" +

                "    </style>\n" +
                "    \n" +
                "    <title>" + input + "</title>\n" +
                "    </head>\n" +
                "\n" +
                "    <body>\n" +
                "            <div class=\"upper-part\">\n" +
                "                <div class=\"search-bar\">\n" +
                "                    <form action=\"search\" method=\"GET\" id=\"SearchSentence\" class=\"form-inline\">\n" +
                "                        <div class=\"form-group mx-sm-3 mb-2\">\n" +
                "                          <input type=\"text\" class=\"form-control shadows\" id=\"Searchvalue\" placeholder=\"\" name=\"Searchvalue\" style=\"width: 1000px;\" value=\"" + input + "\">\n" +
                "                        </div>\n" +
                "                        <input type=\"submit\" class=\"btn btn-primary mb-2\" name=\"SearchInput\" id=\"SearchInput\" value=\"Search\" />\n" +
                "                    </form>\n" +
                "                </div>\n" +
                "            </div>\n" +

                "        <br>\n" +
                "        <hr class=\"style17\">" +
                "\n" +

                "        <div class=\"results\">\n" +
                "            <p class=\"text-muted\" style=\"padding-left: 30px;\">" + results.size() + " results</p>\n";

        //int numOfPages= results.size()/10;




        for (int j = 0; j < results.size(); ++j) {

            page +=
                    "            <div class=\"list-group\">\n" +
                            "                  <small class=\"text-muted\">" + results.get(j).url + "</small>\n" +
                            "                <a href=\"" + results.get(j).url+ "\" class=\" list-group-item-action flex-column align-items-start result-item\">\n" +
                            "                    <h5 class=\"mb-1\" style=\"color: blue;\">" + results.get(j).title + "</h5>\n" +
                            "                </a>\n" +
                            "                  <div class=\"d-flex w-100 justify-content-between\">\n" +
                            "                  </div>\n" +
                            "                  <p class=\"mb-1\">" + results.get(j).plainText + "</p>\n" +
                            "            </div>\n";



        }
        /*int numOfPages= 10;
        for (int i = 1; i <= numOfPages; i++) {
            page +=
                    "      <div class=\"div-results\" >\n"+
                            "<form action=\"pageReq"+i+"\" method=\"GET\" id=\"pageReq"+i+"\">\n" +
                            "      <div class=\"div-pages\" >\n" +
                            "      <ul style=\"list-style-type:none\" id=\"butt\" class=\"nav\">\n" +
                            "<li style=\"display:inline\"><button class=\"page-numbers\" type=\"num_of_pages\" name=\"kb\" value=\"button" +
                            i +
                            "\">" +
                            i +
                            "</button></li>"+
                            "         </ul>\n" +
                            "         </div>\n" +
                            "        </form>"+
                            "         </div>\n" +
                            "         </div>\n" +
            "         </div>\n" ;

        }*/
        page +=

                "        \n" +
                        "    </body>\n" +
                        "\n" +
                        "</html>";


        response.getWriter().println(page);
    }
}



