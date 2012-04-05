﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using Microsoft.Win32;
using System.ComponentModel;
using System.Reflection;
using RazorDB;
using RazorView.Properties;

namespace RazorView {
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window {
        public MainWindow() {
            InitializeComponent();

            List<string> errorsList = new List<string>();
            foreach (var assem in Settings.Default.VisualizerAssembliesList) {
                try {
                    InternalAddVisualizer(assem);
                } catch (Exception) {
                    errorsList.Add(assem);
                }
            }
            foreach (var assem in errorsList) {
                Settings.Default.RemoveVisualizerAssembly(assem);
            }
            LoadVisualizers();
        }

        private void MenuItem_Close(object sender, RoutedEventArgs e) {
            this.Close();
        }

        protected override void OnClosed(EventArgs e) {
            base.OnClosed(e);
            Environment.Exit(0);
        }

        private void MenuItem_OpenDB(object sender, RoutedEventArgs e) {
            var dlg = new OpenFileDialog();
            dlg.CheckFileExists = false;
            dlg.CheckPathExists = false;
            if ((bool) dlg.ShowDialog(this)) {
                OpenDatabase(dlg.FileName);
            }
        }

        private void OpenDatabase(string journalFile) {
            var db = new DBController(journalFile, _factories);

            var tab = new TabItem();
            tab.Header = System.IO.Path.GetDirectoryName(journalFile).Split('\\').Last();

            var ctxMenu = new ContextMenu();
            var menuItem = new MenuItem();
            menuItem.Header = "Close " + tab.Header;
            menuItem.Click += new RoutedEventHandler( (object sender, RoutedEventArgs e) => {
                var dbc = (DBController)tab.Tag;
                dbc.Close();
                tabControl.Items.Remove(tab);
            });
            ctxMenu.Items.Add(menuItem);
            tab.ContextMenu = ctxMenu;

            //var textBox = new TextBox();
            //textBox.AppendText(db.GetAnalysisText());
            
            var grid = new DataGrid();
            grid.AutoGenerateColumns = false;

            var numColumn = new DataGridTextColumn();
            numColumn.IsReadOnly = true;
            numColumn.Header = "";
            numColumn.Binding = new Binding("Index");
            grid.Columns.Add(numColumn);

            var keyColumn = new DataGridTextColumn();
            keyColumn.IsReadOnly = true;
            keyColumn.Header = "Key Bytes";
            keyColumn.Binding = new Binding("Key");
            grid.Columns.Add(keyColumn);

            var valColumn = new DataGridTextColumn();
            valColumn.IsReadOnly = true;
            valColumn.Header = "Value Bytes";
            valColumn.Binding = new Binding("Value");
            grid.Columns.Add(valColumn);
            
            grid.ItemsSource = db.Records;

            //var infoPanel = new StackPanel();
            //infoPanel.Children.Add(textBox);
            //infoPanel.Children.Add(grid);

            tab.Content = grid;
            tab.Tag = db;
            tabControl.Items.Add(tab);

            tab.Focus();
        }

        private List<Assembly> _vizAssemblies = new List<Assembly>();
        private List<IDataVizFactory> _factories = new List<IDataVizFactory>();

        private void MenuItem_AddViz(object sender, RoutedEventArgs e) {
            var dlg = new OpenFileDialog();
            dlg.DefaultExt = "*.dll";
            if ((bool) dlg.ShowDialog(this)) {
                InternalAddVisualizer(dlg.FileName);
                Settings.Default.AddVisualizerAssembly(dlg.FileName);
                Settings.Default.Save();
                LoadVisualizers();
            }
        }

        private void MenuItem_Clear(object sender, RoutedEventArgs e) {
            _vizAssemblies.Clear();
            _factories.Clear();
            Settings.Default.VisualizerAssemblies = "";
            Settings.Default.Save();
            LoadVisualizers();
        }

        private void InternalAddVisualizer(string assemblyFile) {
            try {
                var a = Assembly.LoadFrom(assemblyFile);
                _vizAssemblies.Add(a);

                foreach (var type in a.GetExportedTypes()) {
                    if (type.GetInterfaces().Contains(typeof(IDataVizFactory))) {
                        _factories.Add((IDataVizFactory)Activator.CreateInstance(type));
                    }
                }
            } catch (Exception ex) {
                MessageBox.Show(ex.Message, "Exception", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void LoadVisualizers() {

            menuVizList.Items.Clear();
            foreach (var assem in Settings.Default.VisualizerAssembliesList) {
                MenuItem mi = new MenuItem();
                mi.Header = assem;
                menuVizList.Items.Add(mi);
            }
            if (Settings.Default.VisualizerAssembliesList.Count > 0) {
                menuVizList.Items.Add(new Separator());
            }
            menuVizList.Items.Add(menuAddViz);
            menuVizList.Items.Add(menuClearViz);

        }

    }
}