module.exports= function(eleventyConfig)
{


return   {

    dir:{
        input: "src",
        includes: "_includes",
        output: "dist"
        },

        templateFormats: ["njk", "md", "ejs","11ty.js"],


        htmlTemplateEngine: "njk",
        markdownTemplateEngine: "njk"
    



}



}

